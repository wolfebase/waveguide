package live

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPlaylistGateWakesForTheNextPart(t *testing.T) {
	g := newPlaylistGate()
	g.publish(0, 0, 0)
	if g.ready(0, 0) {
		t.Fatal("an empty open segment is not ready")
	}
	done := make(chan struct{})
	go func() {
		g.wait(0, 0, time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	g.publish(0, 0, 1)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the gate did not wake for part 0")
	}
	if !g.ready(0, 0) || g.ready(0, 1) {
		t.Fatal("only the published part is ready")
	}
	start := time.Now()
	g.wait(3, 0, 80*time.Millisecond)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("a missing part waited past the timeout")
	}
	start = time.Now()
	g.wait(-1, 0, time.Second)
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("a negative sequence should not wait")
	}
}

func TestPackListsAPartBeforeTheSegmentCloses(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.ts")
	gen := exec.Command(ffmpeg, "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=size=320x180:rate=30", "-f", "lavfi", "-i", "sine=frequency=440",
		"-t", "5", "-c:v", "libx264", "-preset", "ultrafast", "-g", "60", "-pix_fmt", "yuv420p", "-c:a", "aac",
		"-output_ts_offset", "95000", "-f", "mpegts", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("source: %v %s", err, out)
	}
	out := filepath.Join(dir, "rendition")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	cmd := exec.Command(ffmpeg, RenditionArgs(0, Source{VideoCodec: "H264", AudioCodec: "AAC", Progressive: true}, Rendition{Video: "copy", Audio: "copy"}, "libx264", "")...)
	cmd.Dir = out
	cmd.Stdin = in
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Capture the fragment stream, then feed it one fragment at a time.
	raw, err := io.ReadAll(stdout)
	waitErr := cmd.Wait()
	if err != nil || waitErr != nil {
		t.Fatalf("encode: %v %v", err, waitErr)
	}
	boxes := topBoxes(raw)
	pr, pw := io.Pipe()
	packErr := make(chan error, 1)
	go func() { packErr <- Pack(out, pr, nil) }()

	wroteFragment := false
	for _, box := range boxes {
		if _, err := pw.Write(box); err != nil {
			t.Fatal(err)
		}
		if string(box[4:8]) == "mdat" {
			wroteFragment = true
			break
		}
	}
	if !wroteFragment {
		t.Fatal("the encode produced no fragment")
	}
	var playlist string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(filepath.Join(out, "index.m3u8"))
		if err == nil && strings.Contains(string(b), "#EXT-X-PART:") {
			playlist = string(b)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if playlist == "" {
		t.Fatal("no part was listed before the rest of the stream")
	}
	if strings.Contains(playlist, "#EXTINF") {
		t.Fatalf("the first fragment closed a segment:\n%s", playlist)
	}
	partDur := 0.0
	for _, line := range strings.Split(playlist, "\n") {
		v, ok := strings.CutPrefix(line, "#EXT-X-PART:DURATION=")
		if !ok {
			continue
		}
		if i := strings.IndexByte(v, ','); i >= 0 {
			v = v[:i]
		}
		partDur, err = strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	// This source's group of pictures is two seconds. Advertising 0.500 would
	// make the player treat the whole fragment as half a second.
	hold := 0.0
	if i := strings.Index(playlist, "PART-HOLD-BACK="); i >= 0 {
		_, _ = fmt.Sscanf(playlist[i+len("PART-HOLD-BACK="):], "%f", &hold)
	}
	if partDur < 1.5 || hold+0.001 < partDur*3 || !strings.Contains(playlist, "CAN-BLOCK-RELOAD=YES") {
		t.Fatalf("part %.3f hold %.3f, playlist:\n%s", partDur, hold, playlist)
	}
	fixed := time.Date(2026, 9, 26, 4, 0, 0, 0, time.UTC)
	tl := NewTimeline()
	tl.now = func() time.Time { return fixed }
	var stamper playlistStamper
	stamped := string(stamper.stamp(out, []byte(playlist), tl))
	if !strings.Contains(stamped, "#EXT-X-PART:") {
		t.Fatalf("stamping dropped the part:\n%s", stamped)
	}
	earliest, ok := tl.Earliest()
	if !ok || !earliest.Equal(fixed.Add(-4*time.Second)) {
		t.Fatalf("the first part should anchor the clock, got %v %v", earliest, ok)
	}

	if _, err := pw.Write(restAfter(boxes)); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()
	if err := <-packErr; err != nil {
		t.Fatal(err)
	}
	final, err := os.ReadFile(filepath.Join(out, "index.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(final), "seg00000.m4s") {
		t.Fatalf("segment 0 missing:\n%s", final)
	}
	assertSegmentCuts(t, string(final))
}

func restAfter(boxes [][]byte) []byte {
	seen := false
	var b []byte
	for _, box := range boxes {
		if !seen {
			if string(box[4:8]) == "mdat" {
				seen = true
			}
			continue
		}
		b = append(b, box...)
	}
	return b
}

func topBoxes(b []byte) [][]byte {
	var out [][]byte
	for len(b) >= 8 {
		size := int(binary.BigEndian.Uint32(b[:4]))
		head := 8
		if size == 1 {
			if len(b) < 16 {
				break
			}
			size = int(binary.BigEndian.Uint64(b[8:16]))
			head = 16
		}
		if size < head || size > len(b) {
			break
		}
		out = append(out, append([]byte(nil), b[:size]...))
		b = b[size:]
	}
	return out
}

func assertSegmentCuts(t *testing.T, playlist string) {
	t.Helper()
	var durs []float64
	for _, line := range strings.Split(playlist, "\n") {
		v, ok := strings.CutPrefix(line, "#EXTINF:")
		if !ok {
			continue
		}
		v = strings.TrimSuffix(v, ",")
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatal(err)
		}
		durs = append(durs, f)
	}
	if len(durs) < 3 {
		t.Fatalf("expected at least three segments, got %v", durs)
	}
	// A segment is one fragment: a half-second transcode or one source GOP.
	// The tail can be short because the encode ended mid-fragment.
	body := durs
	if body[len(body)-1] < 0.3 {
		body = body[:len(body)-1]
	}
	for _, d := range body {
		if d < 0.3 || d > 2.6 {
			t.Errorf("segment duration %.3f is not one fragment (%v)", d, durs)
		}
	}
	if strings.Contains(playlist, "DISCONTINUITY") {
		t.Errorf("a steady encode was marked as a jump:\n%s", playlist)
	}
}

// mp4Box is a short ISO-BMFF box. The body does not include the size or type.
func mp4Box(kind string, body []byte) []byte {
	b := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(b[:4], uint32(len(b)))
	copy(b[4:8], kind)
	copy(b[8:], body)
	return b
}

// keyframeFragment is one video fragment at pts, lasting dur ticks at 90 kHz.
func keyframeFragment(pts int64, dur uint32) []byte {
	tfhd := make([]byte, 8)
	binary.BigEndian.PutUint32(tfhd[4:8], 1)
	tfdt := make([]byte, 12)
	tfdt[0] = 1
	binary.BigEndian.PutUint64(tfdt[4:12], uint64(pts))
	const trFlags = 0x104 // sample duration and first-sample flags
	trun := make([]byte, 16)
	trun[1] = byte(trFlags >> 16)
	trun[2] = byte(trFlags >> 8)
	trun[3] = byte(trFlags & 0xff)
	binary.BigEndian.PutUint32(trun[4:8], 1)
	binary.BigEndian.PutUint32(trun[12:16], dur)
	traf := append(append(mp4Box("tfhd", tfhd), mp4Box("tfdt", tfdt)...), mp4Box("trun", trun)...)
	moof := mp4Box("moof", append(mp4Box("mfhd", make([]byte, 8)), mp4Box("traf", traf)...))
	return append(moof, mp4Box("mdat", []byte{0})...)
}

func videoInit() []byte {
	tkhd := make([]byte, 24)
	binary.BigEndian.PutUint32(tkhd[12:16], 1)
	mdhd := make([]byte, 24)
	binary.BigEndian.PutUint32(mdhd[12:16], 90000)
	hdlr := make([]byte, 12)
	copy(hdlr[8:12], "vide")
	mdia := append(mp4Box("mdhd", mdhd), mp4Box("hdlr", hdlr)...)
	trak := append(mp4Box("tkhd", tkhd), mp4Box("mdia", mdia)...)
	return append(mp4Box("ftyp", []byte("isom")), mp4Box("moov", mp4Box("trak", trak))...)
}

func packKeyframes(t *testing.T, dir string, pts []int64, dur uint32) string {
	t.Helper()
	var raw []byte
	raw = append(raw, videoInit()...)
	for _, p := range pts {
		raw = append(raw, keyframeFragment(p, dur)...)
	}
	if err := Pack(dir, bytes.NewReader(raw), nil); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func extinfSum(playlist string) (n int, seconds float64) {
	for _, line := range strings.Split(playlist, "\n") {
		v, ok := strings.CutPrefix(line, "#EXTINF:")
		if !ok {
			continue
		}
		v = strings.TrimSuffix(v, ",")
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return n, seconds
		}
		n++
		seconds += f
	}
	return n, seconds
}

// A segment name cut from the playlist used to keep that whole playlist
// alive in the clock map. Each new segment pinned one more copy, so a long
// window grew with the square of its length.
func TestStampDoesNotKeepPlaylistText(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "init.mp4"), videoInit(), 0o644); err != nil {
		t.Fatal(err)
	}
	const n = 160
	const pad = 4096
	for i := 0; i < n; i++ {
		seg := keyframeFragment(int64(i)*90000, 90000)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("seg%05d.m4s", i)), seg, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var st playlistStamper
	tl := NewTimeline()
	marker := strings.Repeat("Q", pad)
	heap := func() uint64 {
		runtime.GC()
		debug.FreeOSMemory()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	for i := 1; i <= n; i++ {
		var b strings.Builder
		b.WriteString("#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:1\n")
		for j := 0; j < i; j++ {
			fmt.Fprintf(&b, "#%s\n#EXTINF:1.000,\nseg%05d.m4s\n", marker, j)
		}
		st.stamp(dir, []byte(b.String()), tl)
	}
	if len(st.cache) != n {
		t.Fatalf("cached %d of %d segments", len(st.cache), n)
	}
	after := heap()
	var grew uint64
	if after > before {
		grew = after - before
	}
	// The retained history is about n²/2 markers. The names themselves are not.
	history := uint64(n*(n+1)/2) * pad
	if grew > history/10 {
		t.Fatalf("stamp kept %d bytes after %d playlists; a retained history is about %d", grew, n, history)
	}
}

// Half-second segments must not bring the rewind window down to a count of
// 2700 (that was 90 minutes only while every segment was 2 seconds).
func TestLiveWindowKeepsNinetyMinutes(t *testing.T) {
	dir := t.TempDir()
	const half = 45000
	pts := make([]int64, 2702)
	for i := range pts {
		pts[i] = int64(i) * half
	}
	short := packKeyframes(t, dir, pts, half)
	n, seconds := extinfSum(short)
	if n != 2702 || seconds < 1350 || seconds > 1352 {
		t.Fatalf("short playlist kept %d segments, %.3fs:\n%s", n, seconds, headTail(short))
	}
	if _, err := os.Stat(filepath.Join(dir, "seg00000.m4s")); err != nil {
		t.Fatal("the oldest short segment was dropped before 90 minutes")
	}
	if _, err := os.Stat(filepath.Join(dir, "seg02701.m4s")); err != nil {
		t.Fatal(err)
	}

	longDir := t.TempDir()
	const halfHour = 30 * 60 * 90000
	longPTS := make([]int64, 6)
	for i := range longPTS {
		longPTS[i] = int64(i) * halfHour
	}
	long := packKeyframes(t, longDir, longPTS, halfHour)
	n, seconds = extinfSum(long)
	if n != 3 || seconds < 5399 || seconds > 5401 {
		t.Fatalf("90 minute window kept %d segments, %.3fs:\n%s", n, seconds, long)
	}
	if !strings.Contains(long, "#EXT-X-MEDIA-SEQUENCE:3\n") {
		t.Fatalf("sequence:\n%s", long)
	}
	if strings.Contains(long, "DISCONTINUITY") {
		t.Fatalf("a 30 minute segment is one group, not a jump:\n%s", long)
	}
	if _, err := os.Stat(filepath.Join(longDir, "seg00000.m4s")); !os.IsNotExist(err) {
		t.Fatalf("segment past 90 minutes still on disk: %v", err)
	}
	if _, err := os.Stat(filepath.Join(longDir, "seg00003.m4s")); err != nil {
		t.Fatal(err)
	}
}

// fragmentAt is one video fragment. flags is the first sample's trun flags
// (0x10000 means it is not a keyframe). pad is extra mdat bytes.
func fragmentAt(pts int64, dur uint32, flags uint32, pad int) []byte {
	tfhd := make([]byte, 8)
	binary.BigEndian.PutUint32(tfhd[4:8], 1)
	tfdt := make([]byte, 12)
	tfdt[0] = 1
	binary.BigEndian.PutUint64(tfdt[4:12], uint64(pts))
	const trFlags = 0x104 // sample duration and first-sample flags
	trun := make([]byte, 16)
	trun[1] = byte(trFlags >> 16)
	trun[2] = byte(trFlags >> 8)
	trun[3] = byte(trFlags & 0xff)
	binary.BigEndian.PutUint32(trun[4:8], 1)
	binary.BigEndian.PutUint32(trun[8:12], flags)
	binary.BigEndian.PutUint32(trun[12:16], dur)
	traf := append(append(mp4Box("tfhd", tfhd), mp4Box("tfdt", tfdt)...), mp4Box("trun", trun)...)
	moof := mp4Box("moof", append(mp4Box("mfhd", make([]byte, 8)), mp4Box("traf", traf)...))
	return append(moof, mp4Box("mdat", make([]byte, pad))...)
}

// A backwards presentation time must close the open segment and mark a
// discontinuity. A second stamp must keep those date-times and the earliest
// anchor: the discontinuity moved the clock the first stamp uses.
func TestTimestampJumpClosesTheSegment(t *testing.T) {
	dir := t.TempDir()
	const step = int64(45000)
	base := int64(2868510171)
	pts := []int64{base, base + step}
	jumped := base + step - 9011*90000
	for i := 0; i < 24; i++ {
		pts = append(pts, jumped+int64(i)*step)
	}
	pr, pw := io.Pipe()
	packErr := make(chan error, 1)
	go func() { packErr <- Pack(dir, pr, nil) }()
	if _, err := pw.Write(videoInit()); err != nil {
		t.Fatal(err)
	}
	for _, p := range pts {
		if _, err := pw.Write(fragmentAt(p, uint32(step), 0, 32<<10)); err != nil {
			t.Fatal(err)
		}
	}
	var playlist string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
		if err == nil && strings.Count(string(b), "#EXTINF") >= 20 && strings.Contains(string(b), "#EXT-X-DISCONTINUITY\n") {
			playlist = string(b)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if playlist == "" {
		b, _ := os.ReadFile(filepath.Join(dir, "index.m3u8"))
		t.Fatalf("the jump did not close segments:\n%s", b)
	}
	if n := strings.Count(playlist, "#EXT-X-DISCONTINUITY"); n != 1 {
		t.Fatalf("discontinuity count %d:\n%s", n, playlist)
	}
	for _, line := range strings.Split(playlist, "\n") {
		v, ok := strings.CutPrefix(line, "#EXTINF:")
		if !ok {
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSuffix(v, ","), 64)
		if err != nil || f > 2 || f < 0.4 {
			t.Fatalf("segment %.3f would stall:\n%s", f, playlist)
		}
	}
	parts, _ := filepath.Glob(filepath.Join(dir, "part*.m4s"))
	if len(parts) > 4 {
		t.Fatalf("open segment kept %d parts", len(parts))
	}
	_ = pw.Close()
	if err := <-packErr; err != nil {
		t.Fatal(err)
	}
	final, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Date(2026, 9, 26, 14, 16, 0, 0, time.UTC)
	tl := NewTimeline()
	tl.now = func() time.Time { return fixed }
	var stamper playlistStamper
	stamped := string(stamper.stamp(dir, final, tl))
	var walls []time.Time
	var afterGap bool
	var gapAt int
	for _, line := range strings.Split(stamped, "\n") {
		if strings.HasPrefix(line, "#EXT-X-DISCONTINUITY") {
			afterGap = true
			continue
		}
		v, ok := strings.CutPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:")
		if !ok {
			continue
		}
		wall, err := time.Parse("2006-01-02T15:04:05.000Z", v)
		if err != nil {
			t.Fatal(err)
		}
		if afterGap && gapAt == 0 {
			gapAt = len(walls)
		}
		afterGap = false
		walls = append(walls, wall)
	}
	if gapAt < 1 || gapAt >= len(walls) {
		t.Fatalf("program times %d, gap at %d:\n%s", len(walls), gapAt, stamped)
	}
	stepWall := walls[gapAt].Sub(walls[gapAt-1])
	if stepWall < 400*time.Millisecond || stepWall > 600*time.Millisecond {
		t.Fatalf("date-time jumped by %s across the discontinuity, want about 0.5s", stepWall)
	}
	earliest, ok := tl.Earliest()
	if !ok || !earliest.Equal(fixed.Add(-4*time.Second)) {
		t.Fatalf("earliest moved to %v", earliest)
	}
	tl.now = func() time.Time { return fixed.Add(30 * time.Second) }
	again := string(stamper.stamp(dir, final, tl))
	if strings.Join(programDates(again), "\n") != strings.Join(programDates(stamped), "\n") {
		t.Fatalf("a later playlist moved date-times:\n%s", again)
	}
	earliest, ok = tl.Earliest()
	if !ok || !earliest.Equal(fixed.Add(-4*time.Second)) {
		t.Fatalf("earliest moved on the second stamp to %v", earliest)
	}
}

func programDates(playlist string) []string {
	var out []string
	for _, line := range strings.Split(playlist, "\n") {
		if strings.HasPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:") {
			out = append(out, line)
		}
	}
	return out
}

func TestForwardJumpAndWrap(t *testing.T) {
	forward := t.TempDir()
	const step = int64(45000)
	pts := []int64{0, step, step + step + 11*90000, step + step + 11*90000 + step}
	pl := packFragments(t, forward, pts, 0, 1)
	if strings.Count(pl, "#EXT-X-DISCONTINUITY") != 1 {
		t.Fatalf("an 11s step is a discontinuity:\n%s", pl)
	}
	fixed := time.Date(2026, 9, 26, 14, 16, 0, 0, time.UTC)
	tl := NewTimeline()
	tl.now = func() time.Time { return fixed }
	var stamper playlistStamper
	first := string(stamper.stamp(forward, []byte(pl), tl))
	second := string(stamper.stamp(forward, []byte(pl), tl))
	if strings.Join(programDates(first), "\n") != strings.Join(programDates(second), "\n") {
		t.Fatalf("an 11s jump moved date-times on the next playlist:\n%s\n---\n%s", first, second)
	}
	wrapped := t.TempDir()
	start := ptsWrap - 2*step
	wrapPTS := []int64{start, start + step, 0}
	if pl := packFragments(t, wrapped, wrapPTS, 0, 1); strings.Contains(pl, "DISCONTINUITY") {
		t.Fatalf("a 33-bit wrap of one group is not a jump:\n%s", pl)
	}
}

func TestOpenRunStaysBounded(t *testing.T) {
	dir := t.TempDir()
	const step = int64(45000)
	// No keyframe, so the half-second cut never fires. The byte cap must.
	pts := make([]int64, 9)
	for i := range pts {
		pts[i] = int64(i) * step
	}
	pl := packFragments(t, dir, pts, 0x10000, 8<<20)
	n, _ := extinfSum(pl)
	if n < 2 {
		t.Fatalf("a run with no keyframe stayed one segment:\n%s", pl)
	}
	parts, _ := filepath.Glob(filepath.Join(dir, "part*.m4s"))
	if len(parts) > 6 {
		t.Fatalf("kept %d parts", len(parts))
	}
}

func TestPackRejectsAHugeBox(t *testing.T) {
	dir := t.TempDir()
	pr, pw := io.Pipe()
	packErr := make(chan error, 1)
	go func() {
		err := Pack(dir, pr, nil)
		_ = pr.Close()
		packErr <- err
	}()
	var read atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pw.Close()
		if _, err := pw.Write(videoInit()); err != nil {
			return
		}
		hdr := make([]byte, 8)
		binary.BigEndian.PutUint32(hdr[:4], 64<<20)
		copy(hdr[4:], "mdat")
		if _, err := pw.Write(hdr); err != nil {
			return
		}
		buf := make([]byte, 1<<20)
		for read.Load() < 48<<20 {
			n, err := pw.Write(buf)
			read.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	select {
	case err := <-packErr:
		if err == nil || !strings.Contains(err.Error(), "fragment larger") {
			t.Fatalf("got %v after reading %d", err, read.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a huge box was still being buffered")
	}
	<-done
	if read.Load() > 40<<20 {
		t.Fatalf("buffered %d bytes", read.Load())
	}
}

func packFragments(t *testing.T, dir string, pts []int64, flags uint32, pad int) string {
	t.Helper()
	var raw []byte
	raw = append(raw, videoInit()...)
	for _, p := range pts {
		raw = append(raw, fragmentAt(p, 45000, flags, pad)...)
	}
	if err := Pack(dir, bytes.NewReader(raw), nil); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func headTail(s string) string {
	if len(s) < 400 {
		return s
	}
	return s[:200] + "\n...\n" + s[len(s)-200:]
}

func TestPartLongerThanASegmentRaisesTargetDuration(t *testing.T) {
	dir := t.TempDir()
	// 0.633s segment and a 1.001s open part. Ceil of the segment alone is 1,
	// and a part target of 1.001 against that is a playlist parse error.
	err := writePacked(dir, []byte("init"),
		[]packedSeg{{name: "seg00000.m4s", dur: 56970}},
		[]packedPart{{name: "part00001.m4s", dur: 90090, sync: true}},
		0, true, false, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if !strings.Contains(text, "#EXT-X-VERSION:9\n") {
		t.Fatalf("parts require version 9:\n%s", text)
	}
	if !strings.Contains(text, "#EXT-X-TARGETDURATION:2\n") || !strings.Contains(text, "PART-TARGET=1.999") {
		t.Fatalf("target duration must cover the part:\n%s", text)
	}
	if !strings.Contains(text, "HOLD-BACK=6.000") || !strings.Contains(text, "PART-HOLD-BACK=5.997") {
		t.Fatalf("hold-back must clear one target duration:\n%s", text)
	}
	if !strings.Contains(text, "DURATION=1.001") {
		t.Fatalf("the part keeps its own duration:\n%s", text)
	}
}

func TestTargetDurationDoesNotShrink(t *testing.T) {
	dir := t.TempDir()
	var hold playlistCeiling
	write := func(seg, part int64) string {
		t.Helper()
		if err := writePacked(dir, []byte("init"),
			[]packedSeg{{name: "seg00000.m4s", dur: seg}},
			[]packedPart{{name: "part00001.m4s", dur: part, sync: true}},
			0, true, false, &hold, nil); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	short := write(45000, 45000)
	if !strings.Contains(short, "#EXT-X-TARGETDURATION:2\n") || !strings.Contains(short, "PART-TARGET=1.999") || !strings.Contains(short, "PART-HOLD-BACK=5.997") || !strings.Contains(short, "HOLD-BACK=6.000") {
		t.Fatalf("short segments still advertise 2s:\n%s", short)
	}
	grown := write(45000, 90090)
	for _, tag := range []string{"#EXT-X-TARGETDURATION:2\n", "PART-TARGET=1.999", "PART-HOLD-BACK=5.997", "HOLD-BACK=6.000", "DURATION=1.001"} {
		if !strings.Contains(grown, tag) {
			t.Fatalf("a longer part under the pin changed %s:\n%s", tag, grown)
		}
	}
	if text := write(45000, 200000); !strings.Contains(text, "#EXT-X-TARGETDURATION:3\n") || !strings.Contains(text, "PART-TARGET=2.222") {
		t.Fatalf("a longer part raises it:\n%s", text)
	}
	if text := write(45000, 45000); !strings.Contains(text, "#EXT-X-TARGETDURATION:3\n") || !strings.Contains(text, "PART-TARGET=2.222") {
		t.Fatalf("it must not shrink:\n%s", text)
	}
}

func TestDeltaPlaylistSkipsTheHead(t *testing.T) {
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:1\n")
	b.WriteString("#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,CAN-SKIP-UNTIL=6.000\n")
	b.WriteString("#EXT-X-MEDIA-SEQUENCE:4\n#EXT-X-MAP:URI=\"init.mp4\"\n")
	for i := 0; i < 20; i++ {
		b.WriteString("#EXT-X-PROGRAM-DATE-TIME:2026-09-26T04:00:0" + strconv.Itoa(i%10) + ".000Z\n")
		b.WriteString("#EXTINF:0.500,\nseg" + strconv.Itoa(i) + ".m4s\n")
	}
	b.WriteString("#EXT-X-PART:DURATION=0.500,URI=\"part00020.m4s\"\n")
	out := string(DeltaPlaylist([]byte(b.String())))
	if !strings.Contains(out, "#EXT-X-SKIP:SKIPPED-SEGMENTS=8\n") {
		t.Fatalf("skip count:\n%s", out)
	}
	if !strings.Contains(out, "#EXT-X-VERSION:9\n") || strings.Contains(out, "seg0.m4s") || !strings.Contains(out, "seg8.m4s") {
		t.Fatalf("delta shape:\n%s", out)
	}
	if !strings.Contains(out, "#EXT-X-MEDIA-SEQUENCE:4\n") || !strings.Contains(out, "part00020.m4s") {
		t.Fatalf("sequence and the open part stay:\n%s", out)
	}
	if strings.Count(out, "#EXTINF") != 12 {
		t.Fatalf("kept %d segments:\n%s", strings.Count(out, "#EXTINF"), out)
	}
	// A jump rides with its segment. Skipping that segment drops the tag.
	// A jump on a segment that is still in the delta stays.
	withGap := strings.Replace(b.String(), "#EXTINF:0.500,\nseg8.m4s\n", "#EXT-X-DISCONTINUITY\n#EXTINF:0.500,\nseg8.m4s\n", 1)
	withGap = strings.Replace(withGap, "#EXTINF:0.500,\nseg1.m4s\n", "#EXT-X-DISCONTINUITY\n#EXTINF:0.500,\nseg1.m4s\n", 1)
	delta := string(DeltaPlaylist([]byte(withGap)))
	if strings.Contains(delta, "seg1.m4s") || strings.Count(delta, "DISCONTINUITY") != 1 || !strings.Contains(delta, "seg8.m4s") {
		t.Fatalf("delta discontinuity:\n%s", delta)
	}
	short := "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:0.500,\nseg00000.m4s\n"
	if got := string(DeltaPlaylist([]byte(short))); got != short {
		t.Fatalf("a short playlist is unchanged:\n%s", got)
	}
}
