package live

import (
	"bytes"
	"io"
)

// programFilterCap is how much of a multiplex to hold while looking for one
// program's map. A PAT is repeated within 100 ms and a PMT within 400 ms,
// which is over a megabyte on a full multiplex. Past this, the bytes go
// through unchanged so a stream we cannot parse still plays.
const programFilterCap = 2 << 20

// programPipe writes one program to a rendition. ffmpeg reads every video
// stream in its input before it emits a frame, and it waits out the probe
// ceiling while any of them still has no picture size. A sibling subchannel
// on the same multiplex does that. Recordings and the scan still see the
// whole multiplex; only the rendition's pipe is narrowed.
type programPipe struct {
	w       io.WriteCloser
	program int
	ready   bool
	pass    bool
	synced  bool
	keep    map[int]bool
	pat     [188]byte
	cc      byte
	hold    []byte
	rest    []byte
}

func newProgramPipe(w io.WriteCloser, program int) io.WriteCloser {
	if w == nil || program <= 0 {
		return w
	}
	return &programPipe{w: w, program: program}
}

func (p *programPipe) Write(chunk []byte) (int, error) {
	n := len(chunk)
	if p.pass {
		_, err := p.w.Write(chunk)
		return n, err
	}
	if !p.ready {
		p.hold = append(p.hold, chunk...)
		if !p.synced {
			if len(p.hold) < 188 {
				return n, nil
			}
			// A rendition that attaches after the tune has started begins
			// mid-packet. Lock once, the way the scan does, and then stay there.
			i := bytes.IndexByte(p.hold[:188], 0x47)
			if i < 0 {
				p.pass = true
				buf := p.hold
				p.hold = nil
				_, err := p.w.Write(buf)
				return n, err
			}
			p.synced = true
			if i > 0 {
				p.hold = append([]byte(nil), p.hold[i:]...)
			}
		}
		if !p.learn(p.hold) {
			if !p.pass && len(p.hold) < programFilterCap {
				return n, nil
			}
			p.pass = true
			buf := p.hold
			p.hold = nil
			_, err := p.w.Write(buf)
			return n, err
		}
		chunk = p.hold
		p.hold = nil
	}
	out, rest := p.filter(chunk)
	p.rest = rest
	if len(out) == 0 {
		return n, nil
	}
	_, err := p.w.Write(out)
	return n, err
}

func (p *programPipe) Close() error {
	if !p.pass && !p.ready && len(p.hold) > 0 {
		_, _ = p.w.Write(p.hold)
		p.hold = nil
	}
	return p.w.Close()
}

func (p *programPipe) learn(buf []byte) bool {
	var tsID, pmtPID int
	absent := false
	for _, sec := range sections(buf, 0) {
		if len(sec) < 8 || sec[0] != 0x00 {
			continue
		}
		// A section cut off by the end of this buffer is not the whole table.
		if sectionEnd(sec)+4 > len(sec) {
			continue
		}
		tsID = int(sec[3])<<8 | int(sec[4])
		if pid := patPMT(sec, p.program); pid != 0 {
			pmtPID = pid
			absent = false
			break
		}
		absent = true
	}
	if pmtPID == 0 {
		// This program is not in a complete table, so holding cannot help.
		if absent {
			p.pass = true
		}
		return false
	}
	var pids []int
	pcr := 0x1FFF
	for _, sec := range sections(buf, pmtPID) {
		if len(sec) < 12 || sec[0] != 0x02 {
			continue
		}
		// A table split across packets is not finished in the first one.
		// Parsing that prefix freezes the stream list without the rest.
		if sectionEnd(sec)+4 > len(sec) {
			continue
		}
		pcr = int(sec[8]&0x1f)<<8 | int(sec[9])
		pids = pmtElementary(sec)
		if len(pids) > 0 {
			break
		}
	}
	if len(pids) == 0 {
		return false
	}
	keep := map[int]bool{0: true, pmtPID: true}
	if pcr != 0x1FFF {
		keep[pcr] = true
	}
	for _, pid := range pids {
		keep[pid] = true
	}
	p.keep = keep
	p.pat = singleProgramPAT(p.program, pmtPID, tsID)
	p.ready = true
	return true
}

func (p *programPipe) filter(data []byte) ([]byte, []byte) {
	if len(p.rest) > 0 {
		data = append(append([]byte(nil), p.rest...), data...)
	}
	var out []byte
	off := 0
	for off+188 <= len(data) {
		if data[off] != 0x47 {
			// One lost sync byte. Stepping by one would slide the phase for good.
			off += 188
			continue
		}
		pkt := data[off : off+188]
		off += 188
		pid := int(pkt[1]&0x1f)<<8 | int(pkt[2])
		if pid == 0 {
			pat := p.pat
			pat[3] = 0x10 | (p.cc & 0x0f)
			p.cc = (p.cc + 1) & 0x0f
			out = append(out, pat[:]...)
			continue
		}
		if p.keep[pid] {
			out = append(out, pkt...)
		}
	}
	return out, append([]byte(nil), data[off:]...)
}

func pmtElementary(sec []byte) []int {
	info := int(sec[10]&0x0f)<<8 | int(sec[11])
	off := 12 + info
	end := sectionEnd(sec)
	var pids []int
	for off+5 <= end {
		pid := int(sec[off+1]&0x1f)<<8 | int(sec[off+2])
		esInfo := int(sec[off+3]&0x0f)<<8 | int(sec[off+4])
		off += 5 + esInfo
		if pid != 0 && pid != 0x1FFF {
			pids = append(pids, pid)
		}
	}
	return pids
}

func singleProgramPAT(program, pmtPID, tsID int) [188]byte {
	body := []byte{
		byte(tsID >> 8), byte(tsID),
		0xc1, 0x00, 0x00,
		byte(program >> 8), byte(program),
		0xe0 | byte(pmtPID>>8), byte(pmtPID),
	}
	n := len(body) + 4
	sec := []byte{0x00, 0xb0 | byte(n>>8), byte(n)}
	sec = append(sec, body...)
	crc := mpegCRC(sec)
	sec = append(sec, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
	var pkt [188]byte
	pkt[0] = 0x47
	pkt[1] = 0x40
	pkt[3] = 0x10
	pkt[4] = 0x00
	copy(pkt[5:], sec)
	for i := 5 + len(sec); i < len(pkt); i++ {
		pkt[i] = 0xff
	}
	return pkt
}

// mpegCRC is the MPEG-2 PSI checksum (CRC-32/MPEG-2), not IEEE.
func mpegCRC(data []byte) uint32 {
	crc := uint32(0xffffffff)
	for _, b := range data {
		crc = (crc << 8) ^ mpegCRCTable[byte(crc>>24)^b]
	}
	return crc
}

var mpegCRCTable = func() [256]uint32 {
	var t [256]uint32
	for i := range t {
		crc := uint32(i) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return t
}()
