package live

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	// partTicks is the shortest segment. A segment starts on a keyframe and
	// stays open until the next one once it has reached this length.
	partTicks = 45000 // 0.5s at 90 kHz
	// windowTicks is how much media a live playlist keeps, about 90 minutes.
	windowTicks = 90 * 60 * 90000
	// jumpTicks is a presentation-time step that is not one more group of
	// pictures. Backwards past half a second, or forwards past this, starts
	// a new timeline. ptsDiff already folds a 33-bit wrap of one group into
	// a normal step, so that wrap is not a jump.
	jumpTicks = 10 * 90000
	// maxOpenBytes bounds the fragments held for the segment that is still
	// open. A backwards timestamp never meets the keyframe close.
	maxOpenBytes = 32 << 20
	// maxBox is the largest incomplete fMP4 box kept in the read buffer.
	// A size field that asks for more is corrupt.
	maxBox = 32 << 20
)

// playlistGate is how a playlist request waits for the next part.
// msn is the media sequence of the open segment. part is the index inside it.
type playlistGate struct {
	mu        sync.Mutex
	cond      *sync.Cond
	origin    int
	openMSN   int
	openParts int
}

func newPlaylistGate() *playlistGate {
	g := &playlistGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *playlistGate) publish(origin, openMSN, openParts int) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.origin = origin
	g.openMSN = openMSN
	g.openParts = openParts
	g.cond.Broadcast()
	g.mu.Unlock()
}

func (g *playlistGate) ready(msn, part int) bool {
	if msn < g.openMSN {
		return true
	}
	return msn == g.openMSN && part >= 0 && g.openParts > part
}

// wait blocks until the playlist contains that segment or part, or the timeout.
// A negative msn returns immediately.
func (g *playlistGate) wait(msn, part int, d time.Duration) {
	if g == nil || msn < 0 {
		return
	}
	deadline := time.Now().Add(d)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ready(msn, part) {
		return
	}
	timer := time.AfterFunc(d, func() {
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	})
	defer timer.Stop()
	for !g.ready(msn, part) {
		if !time.Now().Before(deadline) {
			return
		}
		g.cond.Wait()
	}
}

type packedPart struct {
	name string
	pts  int64
	dur  int64
	body []byte
	sync bool
}

type packedSeg struct {
	name  string
	pts   int64
	dur   int64
	parts []string
	gap   bool
}

// packOutput is called before cmd.Start. startPack runs after Start succeeds.
func packOutput(cmd *exec.Cmd) (io.ReadCloser, *playlistGate, chan struct{}, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	return stdout, newPlaylistGate(), make(chan struct{}), nil
}

func startPack(dir string, stdout io.Reader, gate *playlistGate, done chan struct{}) {
	go func() {
		defer close(done)
		if err := Pack(dir, stdout, gate); err != nil && !os.IsNotExist(err) && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error(fmt.Sprintf("pack %s: %v", dir, err))
		}
	}()
}

// Pack turns an fMP4 fragment stream into init.mp4 and one segment per
// source group of pictures. hls.js will not fetch a part until its buffer is
// already at the live edge, so the first picture waits for a closed segment.
// A keyframe fragment that already names a length of at least half a second
// is that segment. A shorter fragment stays a part until the next keyframe.
func Pack(dir string, r io.Reader, gate *playlistGate) error {
	var init []byte
	var track uint32
	var scale uint32
	var haveInit bool
	var moof []byte
	var open []packedPart
	var closed []packedSeg
	var partN int
	var msn int
	var segGap bool
	var lastPTS, lastDur int64
	var haveLast bool
	allSync := true

	var hold playlistCeiling
	flush := func() error {
		return writePacked(dir, init, closed, open, msn-len(closed), allSync, segGap, &hold, gate)
	}
	closeSeg := func(end int64) error {
		if len(open) == 0 {
			return nil
		}
		name := fmt.Sprintf("seg%05d.m4s", msn)
		var body []byte
		names := make([]string, len(open))
		for i, p := range open {
			body = append(body, p.body...)
			names[i] = p.name
		}
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o644); err != nil {
			return err
		}
		dur := ptsDiff(end, open[0].pts)
		if dur <= 0 {
			dur = partTicks
		}
		closed = append(closed, packedSeg{name: name, pts: open[0].pts, dur: dur, parts: names, gap: segGap})
		segGap = false
		msn++
		open = nil
		var kept int64
		for _, s := range closed {
			kept += s.dur
		}
		for len(closed) > 3 && kept > windowTicks {
			old := closed[0]
			kept -= old.dur
			closed = closed[1:]
			_ = os.Remove(filepath.Join(dir, old.name))
			for _, n := range old.parts {
				_ = os.Remove(filepath.Join(dir, n))
			}
		}
		if len(closed) >= 3 {
			for _, n := range closed[len(closed)-3].parts {
				_ = os.Remove(filepath.Join(dir, n))
			}
		}
		return nil
	}
	take := func(frag []byte) error {
		if !haveInit {
			return nil
		}
		pts, ok := fragmentPTS(frag, track, scale)
		if !ok {
			return nil
		}
		dur := int64(0)
		if d, known := fragmentDuration(frag, track, scale); known {
			dur = d
		}
		sync := fragmentIndependent(frag, track)
		jumped := false
		if haveLast {
			expected := lastPTS
			if lastDur > 0 {
				expected = lastPTS + lastDur
			}
			d := ptsDiff(pts, expected)
			if d < -int64(partTicks) || d > int64(jumpTicks) {
				jumped = true
				slog.Warn(fmt.Sprintf("pack %s: timestamp jump %.3fs", dir, float64(d)/90000))
				if len(open) > 0 {
					if err := closeSeg(expected); err != nil {
						return err
					}
				}
				segGap = true
			}
		}
		if !jumped && len(open) > 0 {
			if open[len(open)-1].dur == 0 {
				step := ptsDiff(pts, open[len(open)-1].pts)
				if step > 0 {
					open[len(open)-1].dur = step
				}
			}
			// Hold a short run of frames until the next keyframe, once the
			// open segment has at least half a second. Closing on a frame
			// that is not a keyframe would start the next segment mid-GOP,
			// and a copy and a transcode would no longer share the cut.
			if sync && ptsDiff(pts, open[0].pts) >= int64(partTicks) {
				if err := closeSeg(pts); err != nil {
					return err
				}
			}
		}
		// A run with no keyframe, or a jump the step check missed, must not
		// keep every fragment. Close on the previous fragment's own end.
		if len(open) > 0 && openBytes(open)+len(frag) > maxOpenBytes {
			end := open[len(open)-1].pts + int64(partTicks)
			if open[len(open)-1].dur > 0 {
				end = open[len(open)-1].pts + open[len(open)-1].dur
			}
			if err := closeSeg(end); err != nil {
				return err
			}
		}
		if len(open) == 0 && !sync {
			allSync = false
		}
		lastPTS, lastDur, haveLast = pts, dur, true
		name := fmt.Sprintf("part%05d.m4s", partN)
		partN++
		if err := os.WriteFile(filepath.Join(dir, name), frag, 0o644); err != nil {
			return err
		}
		open = append(open, packedPart{name: name, pts: pts, dur: dur, body: frag, sync: sync})
		// This fragment is one finished group. Closing it on the next group
		// would hold the first picture for that long, and the cut would be
		// the same one. A short fragment still waits for the next keyframe.
		if sync && dur >= int64(partTicks) && len(open) == 1 {
			if err := closeSeg(pts + dur); err != nil {
				return err
			}
		}
		return flush()
	}

	buf := make([]byte, 0, 1<<20)
	tmp := make([]byte, 32<<10)
	for {
		n, err := r.Read(tmp)
		buf = append(buf, tmp[:n]...)
		for {
			box, rest, ok := peelBox(buf)
			if !ok {
				break
			}
			buf = rest
			kind := string(box[4:8])
			switch kind {
			case "moof":
				moof = box
			case "mdat":
				if moof == nil {
					break
				}
				frag := append(append([]byte{}, moof...), box...)
				moof = nil
				if err := take(frag); err != nil {
					return err
				}
			default:
				if moof == nil && !haveInit {
					init = append(init, box...)
					if kind == "moov" {
						id, sc, ok := videoTrack(init)
						if !ok {
							return fmt.Errorf("pack: no video track")
						}
						track, scale = id, sc
						if err := os.WriteFile(filepath.Join(dir, "init.mp4"), init, 0o644); err != nil {
							return err
						}
						haveInit = true
					}
				}
			}
		}
		if len(buf) > maxBox {
			return fmt.Errorf("pack: fragment larger than %d bytes", maxBox)
		}
		if err != nil {
			if err == io.EOF {
				if len(open) > 0 {
					end := open[len(open)-1].pts + partTicks
					if open[len(open)-1].dur > 0 {
						end = open[len(open)-1].pts + open[len(open)-1].dur
					}
					if err := closeSeg(end); err != nil {
						return err
					}
					return flush()
				}
				return nil
			}
			return err
		}
	}
}

func peelBox(b []byte) (box, rest []byte, ok bool) {
	if len(b) < 8 {
		return nil, b, false
	}
	size := int(binary.BigEndian.Uint32(b[:4]))
	head := 8
	if size == 1 {
		if len(b) < 16 {
			return nil, b, false
		}
		size = int(binary.BigEndian.Uint64(b[8:16]))
		head = 16
	}
	if size < head || size > len(b) {
		return nil, b, false
	}
	return b[:size], b[size:], true
}

func openBytes(open []packedPart) int {
	n := 0
	for _, p := range open {
		n += len(p.body)
	}
	return n
}

// playlistCeiling is the longest target this rendition has advertised.
// AVPlayer rejects a reload that changes TARGETDURATION or PART-TARGET
// (CoreMedia -12642), so both stick.
type playlistCeiling struct {
	target int
	part   int
}

func writePacked(dir string, init []byte, closed []packedSeg, open []packedPart, origin int, allSync, segGap bool, hold *playlistCeiling, gate *playlistGate) error {
	if len(init) == 0 {
		return nil
	}
	partTarget := partTicks
	target := partTicks
	for _, s := range closed {
		if s.dur > int64(target) {
			target = int(s.dur)
		}
	}
	for _, p := range open {
		if p.dur > int64(target) {
			target = int(p.dur)
		}
		if p.dur > int64(partTarget) {
			partTarget = int(p.dur)
		}
	}
	// A part can outlast every closed segment. Two seconds covers the
	// one-second segments this packager writes; a longer one sticks.
	const floor = 2 * 90000
	if target < floor {
		target = floor
	}
	if hold != nil && hold.target > target {
		target = hold.target
	}
	// One millisecond under the segment target, unless the open part is
	// already longer. A value copied from the open part grows and shrinks
	// on every reload.
	pinned := target - 90
	if pinned < partTicks {
		pinned = partTicks
	}
	if partTarget < pinned {
		partTarget = pinned
	}
	if partTarget > target {
		partTarget = target
	}
	if hold != nil {
		if hold.target < target {
			hold.target = target
		}
		if hold.part > partTarget && hold.part <= target {
			partTarget = hold.part
		} else if partTarget > hold.part {
			hold.part = partTarget
		}
	}
	var b []byte
	// Parts, part-inf, and server-control require version 9.
	b = append(b, "#EXTM3U\n#EXT-X-VERSION:9\n"...)
	targetSec := (target + 89999) / 90000
	if targetSec < 1 {
		targetSec = 1
	}
	b = append(b, "#EXT-X-TARGETDURATION:"+strconv.Itoa(targetSec)+"\n"...)
	// Part hold-back is three part targets. Full hold-back is three target
	// durations; the part value has to stay under it.
	partHold := 3 * float64(partTarget) / 90000
	if minHold := float64(targetSec); partHold < minHold {
		partHold = minHold
	}
	fullHold := float64(targetSec * 3)
	if partHold >= fullHold {
		partHold = fullHold - 0.001
	}
	b = append(b, fmt.Sprintf("#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=%.3f,PART-HOLD-BACK=%.3f,CAN-SKIP-UNTIL=%d.000\n", fullHold, partHold, targetSec*6)...)
	b = append(b, "#EXT-X-PART-INF:PART-TARGET="+fmtDur(int64(partTarget))+"\n"...)
	if allSync {
		b = append(b, "#EXT-X-INDEPENDENT-SEGMENTS\n"...)
	}
	b = append(b, "#EXT-X-MEDIA-SEQUENCE:"+strconv.Itoa(origin)+"\n"...)
	b = append(b, "#EXT-X-MAP:URI=\"init.mp4\"\n"...)
	for _, s := range closed {
		if s.gap {
			b = append(b, "#EXT-X-DISCONTINUITY\n"...)
		}
		b = append(b, "#EXTINF:"+fmtDur(s.dur)+",\n"+s.name+"\n"...)
	}
	if segGap && len(open) > 0 {
		b = append(b, "#EXT-X-DISCONTINUITY\n"...)
	}
	for _, p := range open {
		dur := p.dur
		if dur <= 0 {
			dur = partTicks
		}
		line := "#EXT-X-PART:DURATION=" + fmtDur(dur)
		if p.sync {
			line += ",INDEPENDENT=YES"
		}
		line += ",URI=\"" + p.name + "\"\n"
		b = append(b, line...)
	}
	tmp := filepath.Join(dir, "index.m3u8.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "index.m3u8")); err != nil {
		return err
	}
	if gate != nil {
		gate.publish(origin, origin+len(closed), len(open))
	}
	return nil
}

func fmtDur(ticks int64) string {
	if ticks <= 0 {
		ticks = partTicks
	}
	return strconv.FormatFloat(float64(ticks)/90000, 'f', 3, 64)
}

// fragmentIndependent reports whether the fragment's first video sample is a
// keyframe. Missing flags are not treated as one: closing on a guess would
// cut a copy and a transcode at different frames.
func fragmentIndependent(seg []byte, track uint32) bool {
	moof := child(seg, "moof")
	for _, t := range boxes(moof) {
		if t.kind != "traf" {
			continue
		}
		tfhd := child(t.body, "tfhd")
		if len(tfhd) < 8 || binary.BigEndian.Uint32(tfhd[4:8]) != track {
			continue
		}
		flags, ok := firstSampleFlags(tfhd, child(t.body, "trun"))
		if !ok {
			return false
		}
		return flags&0x10000 == 0
	}
	return false
}

func firstSampleFlags(tfhd, trun []byte) (uint32, bool) {
	if len(trun) >= 8 {
		trFlags := uint32(trun[1])<<16 | uint32(trun[2])<<8 | uint32(trun[3])
		if trFlags&0x4 != 0 {
			off := 8
			if trFlags&0x1 != 0 {
				off += 4
			}
			if off+4 <= len(trun) {
				return binary.BigEndian.Uint32(trun[off : off+4]), true
			}
		}
	}
	if len(tfhd) < 8 {
		return 0, false
	}
	tfFlags := uint32(tfhd[1])<<16 | uint32(tfhd[2])<<8 | uint32(tfhd[3])
	if tfFlags&0x20 == 0 {
		return 0, false
	}
	off := 8
	if tfFlags&0x1 != 0 {
		off += 8
	}
	if tfFlags&0x2 != 0 {
		off += 4
	}
	if tfFlags&0x8 != 0 {
		off += 4
	}
	if tfFlags&0x10 != 0 {
		off += 4
	}
	if off+4 > len(tfhd) {
		return 0, false
	}
	return binary.BigEndian.Uint32(tfhd[off : off+4]), true
}
