package live

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMPEGCRCMatchesTheSpecVector(t *testing.T) {
	// CRC-32/MPEG-2 check value for the ASCII string "123456789".
	if got := mpegCRC([]byte("123456789")); got != 0x0376e6e7 {
		t.Fatalf("crc %08x", got)
	}
}

func TestProgramFilterDropsASiblingWithoutAPicture(t *testing.T) {
	raw := twoProgramTS(1, 0x1000, 0x110, 0x111, 2, 0x1001, 0x210)
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 1)
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := pidsOf(buf.Bytes())
	for _, pid := range []int{0, 0x1000, 0x110, 0x111} {
		if !got[pid] {
			t.Fatalf("missing pid %d in %v", pid, got)
		}
	}
	if got[0x1001] || got[0x210] {
		t.Fatalf("sibling leaked: %v", got)
	}
	var listed []int
	for _, sec := range sections(buf.Bytes(), 0) {
		if len(sec) < 8 || sec[0] != 0 {
			continue
		}
		end := sectionEnd(sec)
		if end+4 > len(sec) {
			t.Fatalf("short pat %d", len(sec))
		}
		crc := mpegCRC(sec[:end])
		have := uint32(sec[end])<<24 | uint32(sec[end+1])<<16 | uint32(sec[end+2])<<8 | uint32(sec[end+3])
		if crc != have {
			t.Fatalf("pat crc %08x want %08x", have, crc)
		}
		for off := 8; off+4 <= end; off += 4 {
			prog := int(sec[off])<<8 | int(sec[off+1])
			if prog != 0 {
				listed = append(listed, prog)
			}
		}
	}
	if len(listed) != 1 || listed[0] != 1 {
		t.Fatalf("pat programs %v", listed)
	}
}

func TestProgramFilterLearnsAcrossWrites(t *testing.T) {
	raw := twoProgramTS(1, 0x1000, 0x110, 0x111, 2, 0x1001, 0x210)
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 1)
	if _, err := w.Write(raw[:188]); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatal("emitted before the program map")
	}
	if _, err := w.Write(raw[188:]); err != nil {
		t.Fatal(err)
	}
	got := pidsOf(buf.Bytes())
	if !got[0x110] || !got[0x111] || got[0x210] {
		t.Fatalf("pids %v", got)
	}
}

func TestProgramFilterWaitsOutATableInterval(t *testing.T) {
	// More than a megabyte of null packets, then the map. A 256 KiB give-up
	// would pass the sibling through.
	pkt := tsPacket(0x1FFF, false, nil)
	var raw []byte
	for len(raw) < 1<<20+200000 {
		raw = append(raw, pkt...)
	}
	raw = append(raw, twoProgramTS(1, 0x1000, 0x110, 0x111, 2, 0x1001, 0x210)...)
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 1)
	for len(raw) > 0 {
		n := 188 * 49
		if n > len(raw) {
			n = len(raw)
		}
		if _, err := w.Write(raw[:n]); err != nil {
			t.Fatal(err)
		}
		raw = raw[n:]
	}
	got := pidsOf(buf.Bytes())
	if !got[0x110] || !got[0x111] || got[0x210] || got[0x1001] {
		t.Fatalf("pids %v", got)
	}
}

func TestProgramFilterPassesThroughWhenTheProgramIsAbsent(t *testing.T) {
	raw := twoProgramTS(1, 0x1000, 0x110, 0x111, 2, 0x1001, 0x210)
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 99)
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), raw) {
		t.Fatalf("rewrote %d bytes of a multiplex without this program", buf.Len())
	}
}

func TestProgramFilterKeepsAudioSplitAcrossTheProgramMap(t *testing.T) {
	pat := psiPacket(0, psiSection(0x00, append([]byte{0x00, 0x01, 0xc1, 0x00, 0x00}, progPID(1, 0x1000)...)))
	first, second := splitProgramMap(1, 0x1000, 0x110, 0x111)
	video := tsPacket(0x110, true, []byte{0x00, 0x00, 0x01, 0xe0})
	audio := tsPacket(0x111, true, []byte{0x00, 0x00, 0x01, 0xc0})
	sibling := tsPacket(0x210, true, bytes.Repeat([]byte{0xff}, 20))
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 1)
	if _, err := w.Write(append(pat, first...)); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatal("closed the stream list on the first half of the program map")
	}
	rest := append(append(second, video...), audio...)
	rest = append(rest, sibling...)
	if _, err := w.Write(rest); err != nil {
		t.Fatal(err)
	}
	got := pidsOf(buf.Bytes())
	if !got[0x110] || !got[0x111] || got[0x210] {
		t.Fatalf("pids %v", got)
	}
}

func TestProgramFilterLocksSyncPastAPartialPacket(t *testing.T) {
	raw := append(bytes.Repeat([]byte{0x11}, 40), twoProgramTS(1, 0x1000, 0x110, 0x111, 2, 0x1001, 0x210)...)
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 1)
	for len(raw) > 0 {
		n := 100
		if n > len(raw) {
			n = len(raw)
		}
		if _, err := w.Write(raw[:n]); err != nil {
			t.Fatal(err)
		}
		raw = raw[n:]
	}
	got := pidsOf(buf.Bytes())
	if !got[0x110] || !got[0x111] || got[0x210] {
		t.Fatalf("pids %v", got)
	}
}

func splitProgramMap(program, pmtPID, videoPID, audioPID int) (first, second []byte) {
	const infoLen = 166
	body := []byte{
		byte(program >> 8), byte(program),
		0xc1, 0x00, 0x00,
		0xe0 | byte(videoPID>>8), byte(videoPID),
		0xf0 | byte(infoLen>>8), byte(infoLen),
	}
	body = append(body, bytes.Repeat([]byte{0x00}, infoLen)...)
	body = append(body, streamMPEG2, 0xe0|byte(videoPID>>8), byte(videoPID), 0xf0, 0x00)
	body = append(body, streamAC3, 0xe0|byte(audioPID>>8), byte(audioPID), 0xf0, 0x00)
	sec := appendCRC(0x02, body)
	if len(sec) <= 183 {
		panic("program map fits in one packet")
	}
	pay1 := append([]byte{0x00}, sec[:183]...)
	first = tsPacket(pmtPID, true, pay1)
	second = tsPacket(pmtPID, false, sec[183:])
	return first, second
}

func TestProgramFilterPassesASingleStreamThrough(t *testing.T) {
	raw := []byte("not a transport stream")
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 0)
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	if buf.String() != string(raw) {
		t.Fatalf("passthrough %q", buf.String())
	}
}

func TestProgramFilterGivesUpWhenTheMapNeverArrives(t *testing.T) {
	raw := bytes.Repeat([]byte{0x47, 0x1f, 0xff, 0x10}, programFilterCap/4+188)
	var buf bytes.Buffer
	w := newProgramPipe(&closeBuf{&buf}, 7)
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("held the whole multiplex")
	}
	if !bytes.Equal(buf.Bytes(), raw) {
		t.Fatalf("fallback wrote %d of %d", buf.Len(), len(raw))
	}
}

func TestFilteredProgramProbesWithoutTheSibling(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip(err)
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "one.ts")
	cmd := exec.Command(ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=320x240:rate=30:duration=1",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=1",
		"-c:v", "mpeg2video", "-g", "15", "-b:v", "500k",
		"-c:a", "ac3", "-b:a", "96k",
		"-f", "mpegts", src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("sample: %v %s", err, out)
	}
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	program := onlyProgram(t, raw)
	decoy := addSilentVideo(t, raw, program, 99)
	var filtered bytes.Buffer
	w := newProgramPipe(&closeBuf{&filtered}, program)
	if _, err := w.Write(decoy); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	full := probeNotes(t, ffprobe, decoy)
	if !strings.Contains(full, "Could not find codec parameters") && !strings.Contains(full, "unspecified size") {
		t.Fatalf("sibling did not hold the probe:\n%s", full)
	}
	narrow := probeNotes(t, ffprobe, filtered.Bytes())
	if strings.Contains(narrow, "Could not find codec parameters") || strings.Contains(narrow, "unspecified size") {
		t.Fatalf("filtered probe still waited:\n%s", narrow)
	}
	show := probeShow(t, ffprobe, filtered.Bytes())
	if !strings.Contains(show, "video") || !strings.Contains(show, "320") {
		t.Fatalf("picture %q notes %s", show, narrow)
	}
}

type closeBuf struct{ *bytes.Buffer }

func (c closeBuf) Close() error { return nil }

func twoProgramTS(a, aPMT, aVideo, aAudio, b, bPMT, bVideo int) []byte {
	pat := psiPacket(0, psiSection(0x00, append(append([]byte{0x00, 0x01, 0xc1, 0x00, 0x00},
		progPID(a, aPMT)...), progPID(b, bPMT)...)))
	pmtA := psiPacket(aPMT, psiSection(0x02, pmtWithAudio(a, aVideo, aAudio)))
	pmtB := psiPacket(bPMT, psiSection(0x02, pmtBody(b, streamMPEG2, bVideo)))
	video := tsPacket(aVideo, true, []byte{0x00, 0x00, 0x01, 0xe0})
	audio := tsPacket(aAudio, true, []byte{0x00, 0x00, 0x01, 0xc0})
	other := tsPacket(bVideo, true, bytes.Repeat([]byte{0xff}, 20))
	var out []byte
	out = append(out, pat...)
	out = append(out, pmtA...)
	out = append(out, pmtB...)
	out = append(out, video...)
	out = append(out, audio...)
	out = append(out, other...)
	return out
}

func pmtWithAudio(program, videoPID, audioPID int) []byte {
	body := pmtBody(program, streamMPEG2, videoPID)
	body = append(body, streamAC3, 0xe0|byte(audioPID>>8), byte(audioPID), 0xf0, 0x00)
	return body
}

func psiPacket(pid int, section []byte) []byte {
	// psiSection already includes the pointer byte.
	return tsPacket(pid, true, section)
}

func pidsOf(data []byte) map[int]bool {
	got := map[int]bool{}
	for off := 0; off+188 <= len(data); off += 188 {
		if data[off] != 0x47 {
			continue
		}
		pid := int(data[off+1]&0x1f)<<8 | int(data[off+2])
		got[pid] = true
	}
	return got
}

func onlyProgram(t *testing.T, data []byte) int {
	t.Helper()
	for _, sec := range sections(data, 0) {
		if len(sec) < 12 || sec[0] != 0 {
			continue
		}
		end := sectionEnd(sec)
		for off := 8; off+4 <= end; off += 4 {
			prog := int(sec[off])<<8 | int(sec[off+1])
			if prog != 0 {
				return prog
			}
		}
	}
	t.Fatal("no program")
	return 0
}

func addSilentVideo(t *testing.T, raw []byte, keep, extra int) []byte {
	t.Helper()
	const pmtPID = 0x1f00
	const videoPID = 0x1f10
	var tsID int
	var programs []byte
	for _, sec := range sections(raw, 0) {
		if len(sec) < 8 || sec[0] != 0 {
			continue
		}
		tsID = int(sec[3])<<8 | int(sec[4])
		end := sectionEnd(sec)
		if end > 8 {
			programs = append([]byte(nil), sec[8:end]...)
		}
		break
	}
	if len(programs) == 0 {
		t.Fatal("pat")
	}
	programs = append(programs, progPID(extra, pmtPID)...)
	body := []byte{byte(tsID >> 8), byte(tsID), 0xc1, 0x00, 0x00}
	body = append(body, programs...)
	patSec := appendCRC(0x00, body)
	pat := tsPacket(0, true, append([]byte{0x00}, patSec...))
	pmtSec := appendCRC(0x02, pmtBody(extra, streamMPEG2, videoPID))
	pmt := tsPacket(pmtPID, true, append([]byte{0x00}, pmtSec...))
	junk := tsPacket(videoPID, true, bytes.Repeat([]byte{0x11}, 40))
	var out []byte
	replaced := false
	for off := 0; off+188 <= len(raw); off += 188 {
		pkt := raw[off : off+188]
		pid := int(pkt[1]&0x1f)<<8 | int(pkt[2])
		if pid == 0 && !replaced {
			out = append(out, pat...)
			out = append(out, pmt...)
			for range 30 {
				out = append(out, junk...)
			}
			replaced = true
			continue
		}
		if pid == 0 {
			out = append(out, pat...)
			continue
		}
		out = append(out, pkt...)
	}
	if !replaced {
		t.Fatal("no pat packet")
	}
	if keep == 0 {
		t.Fatal("program")
	}
	return out
}

func appendCRC(tableID byte, body []byte) []byte {
	n := len(body) + 4
	sec := []byte{tableID, 0xb0 | byte(n>>8), byte(n)}
	sec = append(sec, body...)
	crc := mpegCRC(sec)
	return append(sec, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
}

func probeNotes(t *testing.T, ffprobe string, data []byte) string {
	t.Helper()
	cmd := exec.Command(ffprobe, "-v", "warning", "-probesize", "8000000", "-analyzeduration", "1000000", "-i", "pipe:0")
	cmd.Stdin = bytes.NewReader(data)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

func probeShow(t *testing.T, ffprobe string, data []byte) string {
	t.Helper()
	cmd := exec.Command(ffprobe, "-v", "error", "-probesize", "8000000", "-analyzeduration", "1000000",
		"-show_entries", "stream=codec_type,width,height", "-of", "csv=p=0", "-i", "pipe:0")
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ffprobe %v %s", err, out)
	}
	return string(out)
}
