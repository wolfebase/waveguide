package live

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"broadwave/internal/disk"
	"broadwave/internal/hdhr"
	"broadwave/internal/psip"
	"broadwave/internal/store"
)

type Tuner struct {
	Index    int    `json:"index"`
	Guide    string `json:"guide,omitempty"`
	Name     string `json:"name,omitempty"`
	Target   string `json:"target,omitempty"`
	Ours     bool   `json:"ours"`
	Strength int    `json:"strength,omitempty"`
	Quality  int    `json:"quality,omitempty"`
	Symbol   int    `json:"symbol,omitempty"`
	Shared   int    `json:"viewers,omitempty"`
	// ATSC3 is set when this tuner can lock an ATSC 3.0 channel.
	// A FLEX 4K sets it on the first two. It is not derived from a guide number.
	ATSC3 bool `json:"-"`
}

// StreamInfo explains what a viewer is getting and why.
type StreamInfo struct {
	Rendition    string `json:"rendition"`
	Video        string `json:"video"`
	Audio        string `json:"audio"`
	Mode         string `json:"mode,omitempty"`
	Reason       string `json:"reason"`
	SourceVideo  string `json:"sourceVideo,omitempty"`
	SourceAudio  string `json:"sourceAudio,omitempty"`
	Encoder      string `json:"encoder,omitempty"`
	Scan         string `json:"scan,omitempty"`
	SourceWidth  int    `json:"sourceWidth,omitempty"`
	SourceHeight int    `json:"sourceHeight,omitempty"`
	SourceFPS    string `json:"sourceFps,omitempty"`
	OutputWidth  int    `json:"outputWidth,omitempty"`
	OutputHeight int    `json:"outputHeight,omitempty"`
	OutputFPS    string `json:"outputFps,omitempty"`
	Bitrate      string `json:"bitrate,omitempty"`
	Decode       string `json:"decode,omitempty"`
}

type Session struct {
	ChannelID int64      `json:"channelId"`
	Playlist  string     `json:"playlist"`
	Rendition string     `json:"rendition"`
	Stream    StreamInfo `json:"stream"`
	Encoder   string     `json:"encoder"`
	Shared    bool       `json:"shared"`
	Viewers   int        `json:"viewers"`
	Frequency int        `json:"frequencyHz"`
	Program   int        `json:"program"`
	Tuners    []Tuner    `json:"tuners,omitempty"`

	// Fields the current web player reads.
	Profile   string   `json:"profile"`
	Audio     string   `json:"audio"`
	Picture   string   `json:"picture,omitempty"`
	VideoMode string   `json:"videoMode"`
	Hints     []string `json:"hints"`

	File string `json:"-"`
}

type BusyError struct {
	Tuners []Tuner
}

func (e *BusyError) Error() string {
	return "every tuner is busy"
}

// PictureError is a transcode the startup budget does not have room for.
type PictureError struct {
	Tiles int
}

func (e *PictureError) Error() string {
	if e.Tiles == 1 {
		return "This server can play 1 picture at once. Stop it to watch another."
	}
	n := e.Tiles
	if n < 1 {
		n = 1
	}
	return fmt.Sprintf("This server can play %d pictures at once. Stop one.", n)
}

// Hub owns the tuners. It tunes a whole frequency once, and every subchannel,
// rendition, and recording on that frequency reads from the same stream.
type Hub struct {
	Store          *store.Store
	Dir            string
	FFmpeg         string
	Encoder        string
	Host           Host
	HEVC           bool
	DeintBroadcast string
	DeintSmooth    string
	OnSaved        func(store.Recording)

	// OnChange is called, outside the hub lock, when viewers, renditions,
	// recordings, or tuners change.
	OnChange func()

	// OnPSIP is called, outside the read loop, when a tuned mux yields a guide.
	OnPSIP func(freqHz int, guide psip.Guide)

	// RenditionIdle is how long a rendition with no viewers keeps running,
	// so flipping back to a channel is instant.
	RenditionIdle time.Duration

	// FallbackWindow is how long a GPU encode has to fail before a death is a
	// late failure (rebuild once) instead of an immediate software fallback.
	// Zero means 8 seconds.
	FallbackWindow time.Duration

	// MoveBudget is how long a vanished device has to hand its stream to
	// another device that can tune the same channel. Zero means 5 seconds.
	MoveBudget time.Duration

	mu         sync.Mutex
	muxes      map[int]*mux
	channels   map[int64]*feed
	reserved   map[int]bool
	hold       int
	next       int
	scanCancel context.CancelFunc
	scanToken  *struct{}
	playMu     sync.Mutex
	plays      map[int64]struct{}
}

type mux struct {
	freq   int
	tuner  int
	host   string
	base   string
	device string
	body   io.ReadCloser
	// moved is set once a dead device has tried to hand the stream off.
	// The budget can walk every other device. A second read error does not.
	moved    bool
	input    string
	cancel   context.CancelFunc
	feeds    map[string]*feed
	pipes    []*pipeSub
	programs []hdhr.Program
	pipeMu   sync.Mutex
	// lead is the opening of the tune. An encode that attaches while it is
	// current reads a copy, including one restarted for film or a later
	// audio stream. Dropping those bytes makes the encoder wait for the
	// next group of pictures.
	lead        []byte
	leadSince   time.Time
	leadDone    bool
	frames      sync.Once
	psip        psip.Harvester
	picMu       sync.Mutex
	picBuf      []byte
	picDone     bool
	picDirty    bool
	picPrograms []int
	pictures    map[int]notedPicture
}

// feed is one channel on a tuned frequency.
type feed struct {
	channel    store.SourceChannel
	program    int
	source     Source
	renditions map[string]*rendition
	recording  *recording
	timeline   *Timeline
	tracks     []AudioTrack
	probing    bool
	// headerOrder is the scan read from this tune's packets. Soft 3:2 is film
	// here even when the stored field order says progressive. ffprobe must not
	// overwrite it: its field_order calls those pictures progressive.
	headerOrder string
	// exports counts raw MPEG-TS readers such as Plex or Jellyfin using the emulated tuner.
	exports int
}

// rendition is one ffmpeg process producing HLS for one delivery form.
type rendition struct {
	spec    Rendition
	dir     string
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	sub     *pipeSub
	viewers int
	seen    time.Time
	idle    *time.Timer
	stamper playlistStamper
	// clock maps this encode onto wall time. A transcode's fMP4 timestamps
	// start at zero, so a second rendition of the channel cannot share the first.
	clock     *Timeline
	fallback  bool
	restarted bool
	// waited is set after cmd.Wait returns, before the hub lock. A stop that
	// arrives in that window must not signal the pid: Wait has reaped it.
	waited atomic.Bool
	args   []string
	// gate blocks a playlist reload until the requested part exists.
	// packDone closes when the packager has finished writing that directory.
	gate     *playlistGate
	packDone chan struct{}
}

type recording struct {
	id    int64
	cmd   *exec.Cmd
	stdin io.WriteCloser
	sub   *pipeSub
	timer *time.Timer
}

type pipeSub struct {
	w    io.WriteCloser
	ch   chan []byte
	done chan struct{}
	once sync.Once
}

func (s *pipeSub) stop() {
	s.once.Do(func() { close(s.done) })
}

func New(st *store.Store, dir, ffmpeg, encoder string) *Hub {
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	if encoder == "" {
		encoder = DetectEncoder(ffmpeg)
	}
	broadcast, smooth := ProbeDeint(ffmpeg, encoder)
	return &Hub{
		Store: st, Dir: dir, FFmpeg: ffmpeg, Encoder: encoder, HEVC: ProbeHEVC(ffmpeg, encoder),
		DeintBroadcast: broadcast, DeintSmooth: smooth,
		RenditionIdle: 20 * time.Second,
		muxes:         map[int]*mux{}, channels: map[int64]*feed{}, reserved: map[int]bool{},
	}
}

func (h *Hub) deintFor(mode, codec string) string {
	if NormalizeMode(mode) == "film" {
		return ""
	}
	if !InterlacedCodec(codec) && codecName(codec) != "h264" {
		return ""
	}
	if NormalizeMode(mode) == "smooth" && h.DeintSmooth != "" {
		return h.DeintSmooth
	}
	return h.DeintBroadcast
}

// SourceOf describes a channel for the stream decision.
func (h *Hub) SourceOf(ctx context.Context, channelID int64) (Source, error) {
	ch, err := h.Store.SourceChannel(ctx, channelID)
	if err != nil {
		return Source{}, err
	}
	return sourceOf(ch), nil
}

func sourceOf(ch store.SourceChannel) Source {
	// Film is decided from the packets of this tune. A stored "film" value is
	// from before that rule and must not keep the channel at 24p.
	order := ch.FieldOrder
	if order == "film" {
		order = ""
	}
	return Source{VideoCodec: ch.VideoCodec, AudioCodec: ch.AudioCodec, Progressive: order == "progressive", Film: false, UserAgent: ch.UserAgent, Referrer: ch.Referrer, Lace: interlacedOrder(order)}
}

// Watch starts or joins one rendition of a channel.
func (h *Hub) Watch(ctx context.Context, channelID int64, want Rendition) (Session, error) {
	h.preemptScan()
	want = want.normalized()
	ch, err := h.Store.SourceChannel(ctx, channelID)
	if err != nil {
		return Session{}, err
	}
	candidates := []store.SourceChannel{ch}
	if ids, err := h.Store.AlternateChannels(ctx, ch.GuideNumber, ch.ID); err == nil {
		for _, id := range ids {
			if alt, err := h.Store.SourceChannel(ctx, id); err == nil {
				candidates = append(candidates, alt)
			}
		}
	}
	var res *http.Response
	var last error
	chosen := -1
	for i, cand := range candidates {
		h.mu.Lock()
		_, tuned := h.channels[cand.ID]
		inUse := h.streamsForDeviceLocked(cand.DeviceID)
		h.mu.Unlock()
		if streamBusy(cand, inUse) && !tuned {
			last = fmt.Errorf("%s", StreamLimitMessage(cand.StreamLimit))
			continue
		}
		if cand.TunerCount == 0 && cand.StreamURL != "" && !hlsStream(cand) && !tuned {
			opened, err := openStream(cand.StreamURL, cand.UserAgent, cand.Referrer)
			if err != nil {
				last = err
				continue
			}
			res = opened
		}
		ch = cand
		chosen = i
		last = nil
		break
	}
	if chosen < 0 {
		if last == nil {
			last = fmt.Errorf("no source has this channel")
		}
		return Session{}, last
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.ensureFeedLocked(ctx, ch, res)
	if err != nil {
		return Session{}, err
	}
	r, err := h.ensureRenditionLocked(f, want)
	if err != nil {
		h.dropIfUnusedLocked(f)
		return Session{}, err
	}
	r.viewers++
	r.seen = time.Now()
	stopTimer(&r.idle)
	h.changed()
	return h.sessionLocked(f, r), nil
}

// Session returns the live session for a channel that is already playing.
func (h *Hub) Session(channelID int64, rendition string) (Session, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := h.channels[channelID]
	if f == nil {
		return Session{}, false
	}
	r := f.renditions[rendition]
	if r == nil {
		return Session{}, false
	}
	return h.sessionLocked(f, r), true
}

// ensureFeedLocked returns the tuned feed for a channel, tuning if needed. It does
// not start ffmpeg; renditions and recordings attach to the feed on demand.
func (h *Hub) ensureFeedLocked(ctx context.Context, ch store.SourceChannel, stream *http.Response) (*feed, error) {
	// New fills these. A hub literal in a test does not, and a write would panic.
	if h.channels == nil {
		h.channels = map[int64]*feed{}
	}
	if h.muxes == nil {
		h.muxes = map[int]*mux{}
	}
	if h.reserved == nil {
		h.reserved = map[int]bool{}
	}
	if f := h.channels[ch.ID]; f != nil {
		if stream != nil {
			stream.Body.Close()
		}
		return f, nil
	}
	if stream != nil {
		return h.addFeedLocked(h.streamMuxLocked(ch, stream.Body, "stream"), ch), nil
	}
	if hlsStream(ch) {
		return h.addFeedLocked(h.hlsMuxLocked(ch), ch), nil
	}
	host := hostOf(ch.BaseURL)
	if ch.FrequencyHz > 0 {
		if m := h.muxes[ch.FrequencyHz]; m != nil {
			return h.addFeedLocked(m, ch), nil
		}
	}
	bases := []string{ch.BaseURL}
	if h.Store != nil {
		if more, err := h.Store.OtherDevices(ctx, ch.GuideNumber, ch.DeviceID); err == nil {
			for _, base := range more {
				if base != "" && base != ch.BaseURL {
					bases = append(bases, base)
				}
			}
		}
	}
	var devices []DeviceTuners
	var last []Tuner
	for _, candidate := range bases {
		tuners, err := h.readTuners(ctx, candidate)
		if err != nil {
			continue
		}
		last = tuners
		devices = append(devices, DeviceTuners{Host: hostOf(candidate), Base: candidate, Tuners: tuners})
	}
	if len(devices) == 0 {
		return nil, fmt.Errorf("the tuner did not answer")
	}
	held := h.reserved
	if h.hold > 0 && len(devices) > 0 {
		held = map[int]bool{}
		for k, v := range h.reserved {
			held[k] = v
		}
		for k, v := range HoldBack(devices[0].Tuners, h.hold) {
			held[k] = v
		}
	}
	picked, tuner, ok := PickTuner(devices, h.usedTunersLocked(devices[0].Host), held, NeedFor(ch.VideoCodec, ch.AudioCodec, ch.ATSC3))
	if !ok {
		return nil, &BusyError{Tuners: last}
	}
	host = hostOf(picked)
	base := ch.BaseURL
	if strings.Contains(picked, "://") {
		base = picked
	}
	h.reserved[tuner] = true
	defer delete(h.reserved, tuner)
	if base != ch.BaseURL {
		if id := h.deviceID(ctx, base); id != "" {
			ch.DeviceID = id
			ch.BaseURL = base
		}
	}
	root := h.streamRootFor(ctx, base, ch.GuideNumber)
	// A channel that has already been tuned knows its frequency and program.
	// Opening the mux is the lock. A vchannel probe would lock again, then
	// wait for streaminfo, before this same request.
	if ch.FrequencyHz > 0 && ch.ProgramNum > 0 {
		if body, err := openMux(root, tuner, ch.FrequencyHz); err == nil {
			feed := h.beginMuxLocked(ch, host, base, tuner, ch.FrequencyHz, nil, body)
			// streaminfo on a tuner that is already locked records the other
			// subchannels. A vchannel probe would lock the same frequency again.
			go h.rememberSiblings(host, tuner, ch.FrequencyHz)
			return feed, nil
		}
	}
	freq, programs, err := probe(host, tuner, ch.GuideNumber)
	if err != nil {
		streamURL := strings.TrimRight(root, "/") + "/auto/v" + ch.GuideNumber
		if h.Encoder == "" || h.Encoder == "libx264" {
			if q := hdhr.ExtendQuery(ch.ModelNumber); q != "" {
				streamURL += "?" + q
			}
		}
		res, err := openStream(streamURL, "", "")
		if err != nil {
			return nil, err
		}
		return h.addFeedLocked(h.streamMuxLocked(ch, res.Body, host), ch), nil
	}
	for _, p := range programs {
		_ = h.Store.RememberProgram(ctx, ch.DeviceID, p.GuideNumber, freq, p.Number)
	}
	ch.ProgramNum = programFor(programs, ch.GuideNumber)
	ch.FrequencyHz = freq
	body, err := openMux(root, tuner, freq)
	if err != nil {
		_, _ = hdhr.Control{Addr: controlAddr(host)}.Set(fmt.Sprintf("/tuner%d/channel", tuner), "none")
		return nil, err
	}
	return h.beginMuxLocked(ch, host, base, tuner, freq, programs, body), nil
}

// beginMuxLocked owns the tuner stream and starts the feed. The caller holds h.mu.
func (h *Hub) beginMuxLocked(ch store.SourceChannel, host, base string, tuner, freq int, programs []hdhr.Program, body io.ReadCloser) *feed {
	m := &mux{freq: freq, tuner: tuner, host: host, base: base, device: ch.DeviceID, body: body, feeds: map[string]*feed{}, programs: programs}
	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	h.muxes[freq] = m
	go h.readLoop(runCtx, m)
	h.startFrames(runCtx, m)
	return h.addFeedLocked(m, ch)
}

// rememberSiblings reads streaminfo without changing the channel. The caller
// does not hold h.mu. A sibling that joins before this returns still has its
// own stored program, when it has one.
func (h *Hub) rememberSiblings(host string, tuner, freq int) {
	if h == nil || h.Store == nil || freq == 0 {
		return
	}
	info, err := (hdhr.Control{Addr: controlAddr(host)}).Get(fmt.Sprintf("/tuner%d/streaminfo", tuner))
	if err != nil || strings.TrimSpace(info) == "" {
		return
	}
	programs := hdhr.ParseStreamInfo(info)
	if len(programs) == 0 {
		return
	}
	h.mu.Lock()
	m := h.muxes[freq]
	device := ""
	if m != nil && m.tuner == tuner {
		m.programs = programs
		device = m.device
	}
	h.mu.Unlock()
	if device == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, p := range programs {
		_ = h.Store.RememberProgram(ctx, device, p.GuideNumber, freq, p.Number)
	}
}

// streamMuxLocked wraps a single-program stream (IPTV, or the tuner's /auto URL).
func (h *Hub) streamMuxLocked(ch store.SourceChannel, body io.ReadCloser, host string) *mux {
	h.next--
	m := &mux{freq: h.next, tuner: -1, host: host, device: ch.DeviceID, body: body, feeds: map[string]*feed{}}
	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	h.muxes[m.freq] = m
	go h.readLoop(runCtx, m)
	h.startFrames(runCtx, m)
	return m
}

func (h *Hub) hlsMuxLocked(ch store.SourceChannel) *mux {
	h.next--
	m := &mux{freq: h.next, tuner: -1, host: "hls", device: ch.DeviceID, input: ch.StreamURL, feeds: map[string]*feed{}, cancel: func() {}}
	h.muxes[m.freq] = m
	return m
}

func hlsURL(raw string) bool {
	u := strings.ToLower(raw)
	return strings.Contains(u, ".m3u8")
}

func hlsStream(ch store.SourceChannel) bool {
	switch strings.ToLower(ch.StreamFormat) {
	case "hls":
		return true
	case "mpegts", "ts":
		return false
	default:
		return hlsURL(ch.StreamURL)
	}
}

func (h *Hub) addFeedLocked(m *mux, ch store.SourceChannel) *feed {
	if m.tuner < 0 {
		ch.FrequencyHz = m.freq
		ch.ProgramNum = 0
	} else {
		if ch.ProgramNum == 0 {
			ch.ProgramNum = programFor(m.programs, ch.GuideNumber)
		}
		ch.FrequencyHz = m.freq
	}
	if f := m.feeds[ch.GuideNumber]; f != nil {
		h.channels[ch.ID] = f
		return f
	}
	f := &feed{
		channel: ch, program: ch.ProgramNum, source: sourceOf(ch),
		renditions: map[string]*rendition{}, timeline: NewTimeline(),
	}
	m.feeds[ch.GuideNumber] = f
	h.channels[ch.ID] = f
	m.noteProgram(f.program)
	// A stored scan already chose the graph. Waiting here for the PMT holds
	// the first picture, and the bytes read during that wait never reach
	// ffmpeg. Audio and a late film header are learned beside the encode.
	if m.input == "" && ch.FieldOrder == "" {
		h.learnScanLocked(m, f)
	} else if m.input == "" {
		h.deferScanLocked(m, f)
	}
	if m.input != "" && ch.FieldOrder == "" {
		h.probeInputLocked(m, f)
	}
	return f
}

func (h *Hub) pictureArgs(f *feed, want Rendition) (string, []string) {
	input := "pipe:0"
	if m := muxOf(h, f); m != nil && m.input != "" {
		input = m.input
	}
	args := renditionArgs(f.program, f.sourceFor(want), want, h.Encoder, h.deintFor(want.Mode, f.source.VideoCodec), input)
	return input, args
}

// rebuildRenditionsLocked restarts renditions whose picture graph changed.
// Viewers stay. The old segments go with the process so a bobbed init is not
// served for a progressive or film picture.
func (h *Hub) rebuildRenditionsLocked(f *feed) {
	type kept struct {
		spec    Rendition
		viewers int
		seen    time.Time
	}
	var list []kept
	for key, r := range f.renditions {
		_, next := h.pictureArgs(f, r.spec)
		if slices.Equal(r.args, next) {
			continue
		}
		list = append(list, kept{r.spec, r.viewers, r.seen})
		h.stopRenditionLocked(f, key)
	}
	for _, k := range list {
		r, err := h.ensureRenditionLocked(f, k.spec)
		if err != nil {
			slog.Error(fmt.Sprintf("rebuild %s on %s: %v", k.spec.Key(), f.channel.GuideNumber, err))
			continue
		}
		r.viewers = k.viewers
		r.seen = k.seen
	}
}

func (h *Hub) ensureRenditionLocked(f *feed, want Rendition) (*rendition, error) {
	if want.Codec == "hevc" && !h.HEVC {
		want.Codec = ""
	}
	key := want.Key()
	if r := f.renditions[key]; r != nil {
		return r, nil
	}
	if want.Video != "copy" && h.Host.Tiles > 0 && h.transcodesLocked() >= h.Host.Tiles {
		// A picture nobody is watching still holds its encode for a few seconds.
		// That slot is free for the picture someone is asking for now.
		h.releaseIdleTranscodesLocked()
	}
	if want.Video != "copy" && h.Host.Tiles > 0 && h.transcodesLocked() >= h.Host.Tiles {
		if r := joinTranscode(f, want); r != nil {
			return r, nil
		}
		return nil, &PictureError{Tiles: h.Host.Tiles}
	}
	dir := filepath.Join(h.Dir, "live", fmt.Sprintf("%d", f.channel.ID), key)
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	input, args := h.pictureArgs(f, want)
	cmd := exec.Command(h.FFmpeg, args...)
	cmd.Dir = dir
	var stdin io.WriteCloser
	if input == "pipe:0" {
		var err error
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
	}
	stdout, gate, done, err := packOutput(cmd)
	if err != nil {
		if stdin != nil {
			_ = stdin.Close()
		}
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		if stdin != nil {
			_ = stdin.Close()
		}
		return nil, err
	}
	startPack(dir, stdout, gate, done)
	pid := cmd.Process.Pid
	NotePID(h.Dir, pid)
	r := &rendition{spec: want, dir: dir, cmd: cmd, stdin: stdin, seen: time.Now(), args: args, gate: gate, packDone: done}
	if stdin != nil {
		r.sub = h.attachPipe(muxOf(h, f), newProgramPipe(stdin, f.program), true)
	}
	f.renditions[key] = r
	go h.watchRendition(f, r, pid, encoderOf(h.Encoder, want))
	return r, nil
}

// joinTranscode picks the encode already running on this channel that is
// closest to the size asked for. A smaller one wins a tie, so a tile does not
// jump up to the full picture. A silent tile is not a stand-in for a watch
// with sound, and HEVC is not a stand-in for H.264. Map order is not a choice.
func joinTranscode(f *feed, want Rendition) *rendition {
	rank := map[string]int{"360": 1, "540": 2, "720": 3, "1080": 4}
	wantRank := rank[want.Video]
	var best *rendition
	bestDist, bestRank := 0, 0
	for _, r := range f.renditions {
		if r.spec.Video == "copy" || r.spec.Codec != want.Codec {
			continue
		}
		if want.Audio != "none" && r.spec.Audio == "none" {
			continue
		}
		have := rank[r.spec.Video]
		dist := wantRank - have
		if dist < 0 {
			dist = -dist
		}
		if best == nil || dist < bestDist || (dist == bestDist && have < bestRank) {
			best = r
			bestDist = dist
			bestRank = have
		}
	}
	return best
}

// releaseIdleTranscodesLocked stops transcodes with no viewers so a new
// picture can use the slot. The caller holds h.mu.
func (h *Hub) releaseIdleTranscodesLocked() {
	type idle struct {
		f   *feed
		key string
	}
	var list []idle
	seen := map[*feed]bool{}
	for _, f := range h.channels {
		if seen[f] {
			continue
		}
		seen[f] = true
		for key, r := range f.renditions {
			if r.spec.Video != "copy" && r.viewers == 0 {
				list = append(list, idle{f, key})
			}
		}
	}
	for _, item := range list {
		h.stopRenditionLocked(item.f, item.key)
	}
}

func (h *Hub) transcodesLocked() int {
	// Two channel ids can share one feed. Count that picture once.
	seen := map[*feed]bool{}
	n := 0
	for _, f := range h.channels {
		if seen[f] {
			continue
		}
		seen[f] = true
		for _, r := range f.renditions {
			if r.spec.Video != "copy" {
				n++
			}
		}
	}
	return n
}

func encoderOf(base string, want Rendition) string {
	if want.Video == "copy" {
		return ""
	}
	return OutputEncoder(base, want.Codec)
}

// watchRendition restarts an encode that dies, then releases the tuner if a
// second start also dies. A restart wipes the directory so a software fallback
// never serves the init.mp4 the GPU encode left behind.
func (h *Hub) watchRendition(f *feed, r *rendition, pid int, encoder string) {
	started := time.Now()
	done := r.packDone
	err := r.cmd.Wait()
	r.waited.Store(true)
	ForgetPID(h.Dir, pid)
	// The packager can still be writing the last fragment after ffmpeg exits.
	// A restart wipes the directory, so it waits for those writes first.
	if done != nil {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if f.renditions[r.spec.Key()] != r {
		return
	}
	software := err != nil && time.Since(started) <= h.fallbackWindow() && vaapiFamily(encoder) && !r.fallback
	if err != nil && !r.restarted && h.restartRenditionLocked(f, r, software) {
		slog.Info(fmt.Sprintf("rendition %s on %s restarted after %s", r.spec.Key(), f.channel.GuideNumber, time.Since(started).Round(time.Millisecond)))
		return
	}
	// The process has been waited. Don't signal a pid the OS may have reused.
	r.cmd = nil
	r.stdin = nil
	slog.Error(fmt.Sprintf("rendition %s on %s released after %s: %v", r.spec.Key(), f.channel.GuideNumber, time.Since(started).Round(time.Millisecond), err))
	h.stopRenditionLocked(f, r.spec.Key())
	h.dropIfUnusedLocked(f)
}

func (h *Hub) fallbackWindow() time.Duration {
	if h.FallbackWindow > 0 {
		return h.FallbackWindow
	}
	return 8 * time.Second
}

// restartRenditionLocked starts the encode again in an empty directory.
// software forces the CPU encoder; otherwise the same command line runs once more.
func (h *Hub) restartRenditionLocked(f *feed, r *rendition, software bool) bool {
	// detach closes the pipe. Closing stdin here too races that goroutine.
	if m := muxOf(h, f); m != nil {
		m.detach(r.sub)
	} else if r.sub != nil {
		r.sub.stop()
	}
	r.sub = nil
	r.stdin = nil
	r.stamper.reset()
	r.clock = nil
	if err := os.RemoveAll(r.dir); err != nil {
		return false
	}
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return false
	}
	encoder := encoderOf(h.Encoder, r.spec)
	args := r.args
	if software || len(args) == 0 {
		encoder = "libx264"
		input := "pipe:0"
		if m := muxOf(h, f); m != nil && m.input != "" {
			input = m.input
		}
		args = renditionArgs(f.program, f.sourceFor(r.spec), r.spec, encoder, "", input)
		r.fallback = true
		encoder = OutputEncoder(encoder, r.spec.Codec)
	}
	cmd := exec.Command(h.FFmpeg, args...)
	cmd.Dir = r.dir
	var stdin io.WriteCloser
	if usesPipe(args) {
		var pipeErr error
		stdin, pipeErr = cmd.StdinPipe()
		if pipeErr != nil {
			return false
		}
	}
	stdout, gate, done, pipeErr := packOutput(cmd)
	if pipeErr != nil {
		if stdin != nil {
			_ = stdin.Close()
		}
		return false
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		if stdin != nil {
			_ = stdin.Close()
		}
		return false
	}
	startPack(r.dir, stdout, gate, done)
	next := cmd.Process.Pid
	NotePID(h.Dir, next)
	r.cmd = cmd
	r.stdin = stdin
	r.args = args
	r.gate = gate
	r.packDone = done
	r.restarted = true
	r.waited.Store(false)
	if stdin != nil {
		r.sub = h.attachPipeLocked(muxOf(h, f), newProgramPipe(stdin, f.program))
	}
	go h.watchRendition(f, r, next, encoder)
	return true
}

func usesPipe(args []string) bool {
	for i, arg := range args {
		if arg == "-i" && i+1 < len(args) && args[i+1] == "pipe:0" {
			return true
		}
	}
	return false
}

// EarliestMedia is the newest first-frame program time (Unix ms) among the
// channel's encodes. A fresh tune is a few seconds old. One that has been
// running keeps its original first frame, which is older than the latency target.
func (h *Hub) EarliestMedia(channelID int64) (float64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := h.channels[channelID]
	if f == nil {
		return 0, false
	}
	var best time.Time
	var ok bool
	for _, r := range f.renditions {
		if r == nil || r.clock == nil {
			continue
		}
		at, has := r.clock.Earliest()
		if !has {
			continue
		}
		if !ok || at.After(best) {
			best = at
			ok = true
		}
	}
	if !ok {
		return 0, false
	}
	return float64(best.UnixNano()) / 1e6, true
}

// Playlist returns a rendition's live playlist stamped with that encode's clock.
func (h *Hub) Playlist(channelID int64, key string) ([]byte, error) {
	h.mu.Lock()
	f := h.channels[channelID]
	var r *rendition
	if f != nil {
		r = f.renditions[key]
		if r != nil {
			r.seen = time.Now()
			if r.clock == nil {
				r.clock = NewTimeline()
			}
		}
	}
	clock := (*Timeline)(nil)
	if r != nil {
		clock = r.clock
	}
	h.mu.Unlock()
	if r == nil {
		return nil, os.ErrNotExist
	}
	raw, err := readPlaylist(filepath.Join(r.dir, "index.m3u8"))
	if err != nil {
		return nil, err
	}
	return r.stamper.stamp(r.dir, raw, clock), nil
}

// WaitMedia blocks until the rendition's playlist contains that segment or
// part, or the timeout. The hub lock is not held while waiting.
func (h *Hub) WaitMedia(channelID int64, key string, msn, part int, d time.Duration) {
	h.mu.Lock()
	var gate *playlistGate
	if f := h.channels[channelID]; f != nil {
		if r := f.renditions[key]; r != nil {
			gate = r.gate
			r.seen = time.Now()
		}
	}
	h.mu.Unlock()
	if gate != nil {
		gate.wait(msn, part, d)
	}
}

// Touch records that a viewer of a rendition is still fetching video.
func (h *Hub) Touch(channelID int64, key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if f := h.channels[channelID]; f != nil {
		if r := f.renditions[key]; r != nil {
			r.seen = time.Now()
		}
	}
}

// ReleaseAbandoned drops viewers that stopped asking for video, so a closed
// browser or a sleeping phone does not hold a tuner.
func (h *Hub) ReleaseAbandoned(maxAge time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	for _, f := range h.feedsLocked() {
		for key, r := range f.renditions {
			if r.viewers == 0 || now.Sub(r.seen) < maxAge {
				continue
			}
			r.viewers = 0
			h.stopRenditionLocked(f, key)
		}
		h.dropIfUnusedLocked(f)
	}
}

// Release removes one viewer. An empty key releases from the busiest rendition,
// for clients that don't track which one they joined.
func (h *Hub) Release(channelID int64, key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := h.channels[channelID]
	if f == nil {
		return
	}
	if key == "" {
		best := 0
		for k, r := range f.renditions {
			if r.viewers > best {
				key, best = k, r.viewers
			}
		}
	}
	r := f.renditions[key]
	if r == nil || r.viewers == 0 {
		return
	}
	r.viewers--
	h.changed()
	if r.viewers == 0 {
		stopTimer(&r.idle)
		r.idle = time.AfterFunc(h.RenditionIdle, func() { h.idleStop(channelID, key) })
	}
}

func (h *Hub) idleStop(channelID int64, key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := h.channels[channelID]
	if f == nil {
		return
	}
	if r := f.renditions[key]; r != nil && r.viewers == 0 {
		h.stopRenditionLocked(f, key)
	}
	h.dropIfUnusedLocked(f)
}

func (h *Hub) Record(ctx context.Context, channelID int64, minutes int, title string) (store.Recording, error) {
	return h.RecordMeta(ctx, minutes, store.Recording{ChannelID: channelID, Title: title})
}

// RecordMeta records the original broadcast of a channel. It shares the tuned
// frequency with anyone watching and starts no transcode.
func (h *Hub) RecordMeta(ctx context.Context, minutes int, meta store.Recording) (store.Recording, error) {
	h.preemptScan()
	channelID := meta.ChannelID
	title := meta.Title
	if minutes <= 0 {
		minutes = 60
	}
	if err := h.ensureSpace(ctx); err != nil {
		return store.Recording{}, err
	}
	ch, err := h.Store.SourceChannel(ctx, channelID)
	if err != nil {
		return store.Recording{}, err
	}
	var res *http.Response
	if ch.TunerCount == 0 && ch.StreamURL != "" && !hlsStream(ch) {
		h.mu.Lock()
		_, tuned := h.channels[channelID]
		h.mu.Unlock()
		if !tuned {
			if res, err = openStream(ch.StreamURL, ch.UserAgent, ch.Referrer); err != nil {
				return store.Recording{}, err
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.ensureFeedLocked(ctx, ch, res)
	if _, busy := err.(*BusyError); busy {
		labels, feeds := h.viewerFeedsToPreemptLocked(ch.FrequencyHz, channelID)
		if len(feeds) > 0 {
			if h.Store != nil {
				_ = h.Store.AddEvent(ctx, "recording", StopWarning(labels, time.Now(), title, time.Now()))
			}
			for _, feed := range feeds {
				h.stopFeedLocked(feed)
			}
			f, err = h.ensureFeedLocked(ctx, ch, res)
		}
	}
	if err != nil {
		return store.Recording{}, err
	}
	if f.recording != nil {
		return h.Store.Recording(ctx, f.recording.id)
	}
	dir := filepath.Join(h.Dir, "recordings")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.dropIfUnusedLocked(f)
		return store.Recording{}, err
	}
	name := fmt.Sprintf("%s_%s_%s.ts", time.Now().Format("20060102_150405"), f.channel.GuideNumber, sanitize(f.channel.DisplayName))
	path := uniquePath(filepath.Join(dir, name))
	ends := time.Now().Add(time.Duration(minutes) * time.Minute)
	if title == "" {
		title = f.channel.DisplayName
	}
	if meta.GameID == "" {
		meta.GameID = h.Store.AiringGame(ctx, channelID, title, time.Now())
	}
	id, err := h.Store.CreateRecording(ctx, store.Recording{
		ChannelID: channelID, GuideNumber: f.channel.GuideNumber, Title: title,
		Subtitle: meta.Subtitle, Description: meta.Description, Category: meta.Category, ProgramID: meta.ProgramID, GameID: meta.GameID,
		Path: path, Status: "recording", StartedAt: time.Now(), EndsAt: &ends,
	})
	if err != nil {
		h.dropIfUnusedLocked(f)
		return store.Recording{}, err
	}
	if key := store.EpisodeKey(meta.ProgramID, title, meta.Subtitle, channelID); key != "" {
		_ = h.Store.RememberSeen(ctx, key, false)
	}
	_ = h.Store.AddEvent(ctx, "recording", fmt.Sprintf("Started %s on %s", title, f.channel.GuideNumber))
	input := "pipe:0"
	if mux := muxOf(h, f); mux != nil && mux.input != "" {
		input = mux.input
	}
	cmd := exec.Command(h.FFmpeg, copyArgs(f.program, input, f.channel.UserAgent, f.channel.Referrer, path)...)
	var stdin io.WriteCloser
	if input == "pipe:0" {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			h.abortRecordingLocked(ctx, f, id)
			return store.Recording{}, err
		}
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		h.abortRecordingLocked(ctx, f, id)
		return store.Recording{}, err
	}
	NotePID(h.Dir, cmd.Process.Pid)
	rec := &recording{id: id, cmd: cmd, stdin: stdin}
	if stdin != nil {
		rec.sub = h.attachPipe(muxOf(h, f), stdin, true)
	}
	f.recording = rec
	rec.timer = time.AfterFunc(time.Duration(minutes)*time.Minute, func() { h.StopRecord(id) })
	h.changed()
	return h.Store.Recording(ctx, id)
}

// Shutdown stops every rendition, finishes recordings that are in progress,
// and releases tuners. Used on SIGTERM so a restart does not leave ffmpeg
// running or a tuner locked.
func (h *Hub) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range h.feedsLocked() {
		if f.recording != nil {
			h.finishRecordingLocked(f, "complete", "")
		}
		h.stopFeedLocked(f)
	}
}

// uniquePath adds -2, -3, ... when a recording file already exists. Names are
// per second, and ffmpeg refuses to overwrite, so two recordings of one channel
// started in the same second would otherwise lose the second one.
func uniquePath(path string) string {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return path
	}
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s-%d%s", base, i, ext)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
}

func (h *Hub) StopRecord(id int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range h.feedsLocked() {
		if f.recording != nil && f.recording.id == id {
			h.finishRecordingLocked(f, "complete", "")
			h.dropIfUnusedLocked(f)
			return
		}
	}
}

// ExtendRecording moves the end of an in-progress recording.
func (h *Hub) ExtendRecording(ctx context.Context, id int64, until time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range h.feedsLocked() {
		if f.recording == nil || f.recording.id != id {
			continue
		}
		stopTimer(&f.recording.timer)
		f.recording.timer = time.AfterFunc(time.Until(until), func() { h.StopRecord(id) })
		return h.Store.SetRecordingEnd(ctx, id, until)
	}
	return fmt.Errorf("recording %d is not in progress", id)
}

// viewerFeedsToPreemptLocked chooses one viewer-only frequency a recording may take.
// It does not stop anything. The caller warns, then stops the returned feeds.
// The mux with the highest channel id loses, unless that mux is the recording's station.
func (h *Hub) viewerFeedsToPreemptLocked(keepHz int, keepID int64) (labels []string, feeds []*feed) {
	type group struct {
		feeds []*feed
		maxID int64
	}
	var groups []group
	for _, m := range h.muxes {
		if m == nil || m.tuner < 0 {
			continue
		}
		if keepHz > 0 && m.freq == keepHz {
			continue
		}
		var one []*feed
		maxID := int64(0)
		recording := false
		same := false
		for _, f := range m.feeds {
			if f == nil {
				continue
			}
			if f.recording != nil {
				recording = true
			}
			if keepID > 0 && f.channel.ID == keepID {
				same = true
			}
			if f.channel.ID > maxID {
				maxID = f.channel.ID
			}
			one = append(one, f)
		}
		if recording || same || len(one) == 0 {
			continue
		}
		groups = append(groups, group{feeds: one, maxID: maxID})
	}
	if len(groups) == 0 {
		return nil, nil
	}
	best := 0
	for i := range groups {
		if groups[i].maxID > groups[best].maxID {
			best = i
		}
	}
	feeds = append([]*feed(nil), groups[best].feeds...)
	sort.Slice(feeds, func(i, j int) bool { return feeds[i].channel.ID < feeds[j].channel.ID })
	for _, f := range feeds {
		labels = append(labels, feedLabel(f))
	}
	return labels, feeds
}

func feedLabel(f *feed) string {
	if f.channel.DisplayNumber != "" {
		return f.channel.DisplayNumber
	}
	if f.channel.GuideNumber != "" {
		return f.channel.GuideNumber
	}
	return fmt.Sprintf("%d", f.channel.ID)
}

// SetHold keeps that many tuners free for recordings that are about to start.
func (h *Hub) SetHold(n int) {
	if n < 0 {
		n = 0
	}
	h.mu.Lock()
	h.hold = n
	h.mu.Unlock()
}

func (h *Hub) Tuners(ctx context.Context) ([]Tuner, error) {
	h.mu.Lock()
	host := ""
	for _, m := range h.muxes {
		if m.tuner >= 0 {
			host = m.host
			break
		}
	}
	h.mu.Unlock()
	if h.Store != nil && !strings.Contains(host, "://") {
		devices, err := h.Store.Devices(ctx)
		if err != nil {
			return nil, err
		}
		for _, d := range devices {
			if d.TunerCount > 0 && (host == "" || hostOf(d.BaseURL) == host) {
				host = d.BaseURL
				break
			}
		}
	}
	if host == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.readTuners(ctx, host)
}

func (h *Hub) attachPipeLocked(m *mux, w io.WriteCloser) *pipeSub {
	return h.attachPipe(m, w, false)
}

// leadCap matches the tuner probe. leadFor is how long those opening bytes
// stay useful: the rendition attaches during the scan, which is under a second.
const (
	leadCap = 8 << 20
	leadFor = 2 * time.Second
)

// rememberLead keeps bytes that arrived before an encode or recording attached.
// The buffer stays until leadFor, so a film or audio restart in that window
// can read the same opening. A caller holds no lock.
func (m *mux) rememberLead(chunk []byte) {
	if m == nil || len(chunk) == 0 {
		return
	}
	m.pipeMu.Lock()
	defer m.pipeMu.Unlock()
	m.rememberLeadLocked(chunk)
}

// rememberLeadLocked appends chunk. The caller holds pipeMu. A full or stale
// buffer is dropped rather than spliced onto a later live edge.
func (m *mux) rememberLeadLocked(chunk []byte) {
	if m.leadDone || len(chunk) == 0 {
		return
	}
	if m.leadSince.IsZero() {
		m.leadSince = time.Now()
	}
	if time.Since(m.leadSince) > leadFor || len(m.lead) >= leadCap {
		m.leadDone = true
		m.lead = nil
		return
	}
	room := leadCap - len(m.lead)
	if len(chunk) > room {
		m.leadDone = true
		m.lead = nil
		return
	}
	m.lead = append(m.lead, chunk...)
}

// copyLeadLocked copies the opening for one new subscriber. The caller holds
// pipeMu. The buffer stays for the next encode that attaches in this window.
func (m *mux) copyLeadLocked() []byte {
	if m.leadDone {
		return nil
	}
	if !m.leadSince.IsZero() && time.Since(m.leadSince) > leadFor {
		m.leadDone = true
		m.lead = nil
		return nil
	}
	return append([]byte(nil), m.lead...)
}

// noteLead stores chunk and returns the subscribers that should also see it.
// One lock keeps a new subscriber from receiving that chunk twice.
func (m *mux) noteLead(chunk []byte) []*pipeSub {
	if m == nil {
		return nil
	}
	m.pipeMu.Lock()
	defer m.pipeMu.Unlock()
	m.rememberLeadLocked(chunk)
	return append([]*pipeSub(nil), m.pipes...)
}

func (h *Hub) attachPipe(m *mux, w io.WriteCloser, lead bool) *pipeSub {
	// A few seconds of the mux have to fit. The rendition does not read during
	// VAAPI startup, and a gap at the start leaves the deinterlacer with no
	// picture, so the playlist stays an empty file.
	sub := &pipeSub{w: w, ch: make(chan []byte, 4096), done: make(chan struct{})}
	if m == nil {
		sub.stop()
		_ = w.Close()
		return sub
	}
	m.pipeMu.Lock()
	if lead {
		if head := m.copyLeadLocked(); len(head) > 0 {
			sub.ch <- head
		}
	}
	m.pipes = append(m.pipes, sub)
	m.pipeMu.Unlock()
	go func() {
		defer w.Close()
		for {
			select {
			case <-sub.done:
				return
			case chunk, ok := <-sub.ch:
				if !ok {
					return
				}
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
		}
	}()
	return sub
}

// readLoop is the only reader of the tuner. A slow subscriber loses chunks
// rather than stalling the tuner for everyone else. When the tuner itself
// ends, the mux is released so the tuner does not stay busy.
//
// observeMuxPicture must not take h.mu. learnScanLocked holds that lock while
// it waits for these same packets to reach its subscriber.
func (h *Hub) observeMuxPicture(m *mux, chunk []byte) {
	m.observePicture(chunk)
}

// noteProgram remembers a program whose picture size the panel should learn.
// program 0 is the single program in a filtered stream.
func (m *mux) noteProgram(program int) {
	if program < 0 {
		return
	}
	m.picMu.Lock()
	defer m.picMu.Unlock()
	for _, have := range m.picPrograms {
		if have == program {
			return
		}
	}
	m.picPrograms = append(m.picPrograms, program)
	m.picDirty = true
	m.parseLocked(program)
}

func (m *mux) observePicture(chunk []byte) {
	m.picMu.Lock()
	defer m.picMu.Unlock()
	if m.picDone {
		return
	}
	// A sequence header rides the GOP, which can sit half a second into the mux.
	if len(m.picBuf) < 4<<20 {
		m.picBuf = append(m.picBuf, chunk...)
	}
	full := len(m.picBuf) >= 4<<20
	if !m.picDirty && !full && !m.picturePendingLocked() {
		return
	}
	m.picDirty = false
	for _, program := range m.picPrograms {
		m.parseLocked(program)
	}
	// The window stays open after the first program so a later subchannel on
	// this frequency can still be measured. It closes once the capture is full.
	if full {
		m.picDone = true
	}
}

func (m *mux) picturePendingLocked() bool {
	for _, program := range m.picPrograms {
		if _, ok := m.pictures[program]; !ok {
			return true
		}
	}
	return false
}

func (m *mux) parseLocked(program int) {
	if m.pictures == nil {
		m.pictures = map[int]notedPicture{}
	}
	if _, ok := m.pictures[program]; ok {
		return
	}
	facts, ok := pictureFacts(m.picBuf, program)
	if ok {
		m.pictures[program] = facts
	}
}

func (m *mux) pictureBytes() []byte {
	m.picMu.Lock()
	defer m.picMu.Unlock()
	return append([]byte(nil), m.picBuf...)
}

func (m *mux) picture(program int) (notedPicture, bool) {
	m.picMu.Lock()
	defer m.picMu.Unlock()
	p, ok := m.pictures[program]
	return p, ok && p.Width > 0
}

func (h *Hub) readLoop(ctx context.Context, m *mux) {
	buf := make([]byte, 188*49)
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := m.body.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if g, ok := m.psip.Add(chunk); ok && h.OnPSIP != nil {
				freq, guide := m.freq, g
				go h.OnPSIP(freq, guide)
			}
			h.observeMuxPicture(m, chunk)
			for _, sub := range m.noteLead(chunk) {
				select {
				case sub.ch <- chunk:
				default:
				}
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if h.handOff(ctx, m) {
				continue
			}
			slog.Error(fmt.Sprintf("mux %d ended: %v", m.freq, err))
			h.releaseMux(m)
			return
		}
	}
}

// releaseMux drops every channel on a mux whose tuner read has ended.
func (h *Hub) releaseMux(m *mux) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.muxes[m.freq] != m {
		return
	}
	feeds := make([]*feed, 0, len(m.feeds))
	for _, f := range m.feeds {
		feeds = append(feeds, f)
	}
	for _, f := range feeds {
		if f.recording != nil {
			h.finishRecordingLocked(f, "failed", "The tuner stopped.")
		}
		h.stopFeedLocked(f)
	}
}

func (h *Hub) abortRecordingLocked(ctx context.Context, f *feed, id int64) {
	_ = h.Store.FinishRecording(ctx, id, "failed", "Could not start the recording.")
	h.dropIfUnusedLocked(f)
}

func (h *Hub) sessionLocked(f *feed, r *rendition) Session {
	viewers := 0
	for _, other := range f.renditions {
		viewers += other.viewers
	}
	m := muxOf(h, f)
	shared := viewers > 1 || f.recording != nil || (m != nil && len(m.feeds) > 1)
	spec := r.spec
	key := spec.Key()
	info := StreamInfo{
		Rendition: key, Video: spec.Video, Audio: spec.Audio, Mode: spec.Mode,
		SourceVideo: f.source.VideoCodec, SourceAudio: f.source.AudioCodec,
	}
	if spec.Video != "copy" {
		info.Encoder = encoderOf(h.Encoder, spec)
	}
	var noted notedPicture
	if m != nil {
		noted, _ = m.picture(f.program)
	}
	facts := streamFacts(f.source, f.channel.FieldOrder, spec, info.Encoder, h.deintFor(spec.Mode, f.source.VideoCodec), r.fallback, noted)
	if facts.Encoder != "" {
		info.Encoder = facts.Encoder
	}
	info.Scan = facts.Scan
	info.SourceWidth = facts.SourceWidth
	info.SourceHeight = facts.SourceHeight
	info.SourceFPS = facts.SourceFPS
	info.OutputWidth = facts.OutputWidth
	info.OutputHeight = facts.OutputHeight
	info.OutputFPS = facts.OutputFPS
	info.Bitrate = facts.Bitrate
	info.Decode = facts.Decode
	legacyAudio := "stereo"
	if spec.Audio == "aac6" || spec.Audio == "copy" {
		legacyAudio = "surround"
	}
	videoMode := "transcode"
	if spec.Video == "copy" {
		videoMode = "copy"
	}
	return Session{
		ChannelID: f.channel.ID,
		Playlist:  fmt.Sprintf("/media/live/%d/%s/index.m3u8", f.channel.ID, key),
		Rendition: key,
		Stream:    info,
		Encoder:   h.Encoder,
		Shared:    shared,
		Viewers:   viewers,
		Frequency: f.channel.FrequencyHz,
		Program:   f.program,
		Profile:   renditionProfile(spec.Video),
		Audio:     legacyAudio,
		Picture:   spec.Mode,
		VideoMode: videoMode,
		Hints:     []string{},
		File:      filepath.Join(r.dir, "index.m3u8"),
	}
}

func (h *Hub) stopRenditionLocked(f *feed, key string) {
	r := f.renditions[key]
	if r == nil {
		return
	}
	stopTimer(&r.idle)
	muxOf(h, f).detach(r.sub)
	if r.cmd != nil && r.cmd.Process != nil && !r.waited.Load() {
		ForgetPID(h.Dir, r.cmd.Process.Pid)
		if r.stdin != nil {
			_ = r.stdin.Close()
		}
		_ = r.cmd.Process.Kill()
	}
	delete(f.renditions, key)
	_ = os.RemoveAll(r.dir)
	h.changed()
}

// dropIfUnusedLocked releases the feed, and the tuner with the last feed, once
// nobody is watching or recording it.
func (h *Hub) dropIfUnusedLocked(f *feed) {
	if len(f.renditions) > 0 || f.recording != nil || f.probing || f.exports > 0 {
		return
	}
	h.stopFeedLocked(f)
}

func (h *Hub) stopFeedLocked(f *feed) {
	for key := range f.renditions {
		h.stopRenditionLocked(f, key)
	}
	if h.channels[f.channel.ID] == f {
		delete(h.channels, f.channel.ID)
	}
	for id, other := range h.channels {
		if other == f {
			delete(h.channels, id)
		}
	}
	m := muxOf(h, f)
	if m == nil {
		return
	}
	// muxOf is whatever is stored for this frequency now. A signal read and a
	// field-order probe can both drop the feed this tune replaced; releasing
	// that mux would set the tuner to none under the recording that took it.
	if m.feeds[f.channel.GuideNumber] != f {
		return
	}
	delete(m.feeds, f.channel.GuideNumber)
	if len(m.feeds) == 0 {
		m.cancel()
		if m.body != nil {
			_ = m.body.Close()
		}
		for _, sub := range m.snapshot() {
			sub.stop()
		}
		if m.tuner >= 0 {
			_, _ = hdhr.Control{Addr: m.host}.Set(fmt.Sprintf("/tuner%d/channel", m.tuner), "none")
		}
		delete(h.muxes, m.freq)
	}
}

func (h *Hub) finishRecordingLocked(f *feed, status, errText string) {
	rec := f.recording
	if rec == nil {
		return
	}
	stopTimer(&rec.timer)
	muxOf(h, f).detach(rec.sub)
	if rec.stdin != nil {
		_ = rec.stdin.Close()
	}
	if rec.cmd.Process != nil {
		ForgetPID(h.Dir, rec.cmd.Process.Pid)
		done := make(chan struct{})
		go func() { _ = rec.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = rec.cmd.Process.Kill()
		}
	}
	f.recording = nil
	h.changed()
	_ = h.Store.FinishRecording(context.Background(), rec.id, status, errText)
	if saved, err := h.Store.Recording(context.Background(), rec.id); err == nil {
		writeSidecar(saved)
		message := fmt.Sprintf("Saved %s on %s", saved.Title, saved.GuideNumber)
		if status != "complete" {
			message = fmt.Sprintf("Recording %s on %s ended %s", saved.Title, saved.GuideNumber, status)
		}
		_ = h.Store.AddEvent(context.Background(), "recording", message)
		go h.rememberDuration(saved)
		if h.OnSaved != nil && status == "complete" {
			go h.OnSaved(saved)
		}
	}
}

func (h *Hub) rememberDuration(rec store.Recording) {
	tool := FFProbePath(h.FFmpeg)
	if tool == "" || rec.Path == "" || h.Store == nil {
		return
	}
	seconds, err := ProbeDuration(tool, rec.Path)
	if err != nil || seconds <= 0 {
		_ = h.Store.SetDuration(context.Background(), rec.ID, -1)
		return
	}
	_ = h.Store.SetDuration(context.Background(), rec.ID, seconds)
}

func (h *Hub) ensureSpace(ctx context.Context) error {
	if h.Store == nil || h.Dir == "" {
		return nil
	}
	values, err := h.Store.Settings(ctx)
	if err != nil {
		return nil
	}
	reserve := disk.WatermarkBytes(values["watermarkGB"])
	if reserve == 0 {
		return nil
	}
	dir := filepath.Join(h.Dir, "recordings")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	space, err := disk.Stat(dir)
	if err != nil {
		return nil
	}
	if disk.BelowReserve(space.Free, reserve) {
		_ = h.Store.AddEvent(ctx, "disk", "Refused a recording because free space is under the reserve")
		return &disk.LowError{Free: space.Free, Need: reserve}
	}
	return nil
}

func (h *Hub) changed() {
	if h.OnChange != nil {
		go h.OnChange()
	}
}

func stopTimer(t **time.Timer) {
	if *t != nil {
		(*t).Stop()
		*t = nil
	}
}

// feedsLocked lists each feed once; several channel ids can point at one feed.
func (h *Hub) feedsLocked() []*feed {
	seen := map[*feed]bool{}
	var out []*feed
	for _, f := range h.channels {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].channel.ID < out[j].channel.ID })
	return out
}

// usedTunersLocked is the tuner indexes already streaming on one host.
// Two devices both have a tuner 0. A tune on one must not mark the other busy.
func (h *Hub) usedTunersLocked(host string) map[int]bool {
	want := hostOf(host)
	used := map[int]bool{}
	for _, m := range h.muxes {
		if m.tuner < 0 {
			continue
		}
		if want != "" && hostOf(m.host) != want && hostOf(m.base) != want {
			continue
		}
		used[m.tuner] = true
	}
	return used
}

// StreamLimitMessage is what a viewer sees when a playlist has no free stream.
func StreamLimitMessage(limit int) string {
	return fmt.Sprintf("All %d streams from this playlist are in use. Stop one or raise the limit.", limit)
}

func streamBusy(ch store.SourceChannel, inUse int) bool {
	return ch.TunerCount == 0 && ch.StreamURL != "" && ch.StreamLimit > 0 && inUse >= ch.StreamLimit
}

// StreamsInUse counts channels from one source that are open right now.
func (h *Hub) StreamsInUse(deviceID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.streamsForDeviceLocked(deviceID)
}

func (h *Hub) streamsForDeviceLocked(deviceID string) int {
	n := 0
	for _, f := range h.channels {
		if f.channel.DeviceID == deviceID {
			n++
		}
	}
	return n
}

func (h *Hub) viewersOnTunerLocked(tuner int) int {
	n := 0
	for _, m := range h.muxes {
		if m.tuner != tuner {
			continue
		}
		for _, f := range m.feeds {
			for _, r := range f.renditions {
				n += r.viewers
			}
		}
	}
	return n
}

func (h *Hub) readTuners(ctx context.Context, host string) ([]Tuner, error) {
	raw, err := fetchTunerStatus(ctx, host)
	if err != nil {
		return nil, err
	}
	ours := h.usedTunersLocked(host)
	out := make([]Tuner, 0, len(raw))
	for i, row := range raw {
		out = append(out, Tuner{
			Index: i, Guide: row.VctNumber, Name: row.VctName, Target: row.TargetIP,
			Ours: ours[i], Strength: row.SignalStrengthPercent, Quality: row.SignalQualityPercent, Symbol: row.SymbolQualityPercent,
			Shared: h.viewersOnTunerLocked(i),
		})
	}
	h.markATSC3(ctx, host, out)
	return out, nil
}

// moveBudget is the production limit for handing a live stream to another
// device after the one serving it disappears.
const moveBudget = 5 * time.Second

func (h *Hub) moveLimit() time.Duration {
	if h == nil || h.MoveBudget <= 0 {
		return moveBudget
	}
	return h.MoveBudget
}

// handOff moves a mux whose device has disappeared onto another device that
// can tune the same channel. It walks the other devices until one streams
// or the budget runs out, then a miss releases the dead tuner.
func (h *Hub) handOff(ctx context.Context, m *mux) bool {
	if m == nil || m.moved || m.tuner < 0 {
		return false
	}
	m.moved = true
	ctx, cancel := context.WithTimeout(ctx, h.moveLimit())
	defer cancel()
	if h.moveWithin(ctx, m) {
		return true
	}
	releaseTuner(m.host, m.tuner)
	return false
}

func (h *Hub) moveWithin(ctx context.Context, m *mux) bool {
	if h.Store == nil || ctx.Err() != nil {
		return false
	}
	guide, device, need := h.muxChannel(m)
	if guide == "" {
		return false
	}
	bases, err := h.Store.OtherDevices(ctx, guide, device)
	if err != nil || len(bases) == 0 {
		return false
	}
	skipped := map[string]bool{}
	for tries := 0; tries < 16 && ctx.Err() == nil; tries++ {
		var devices []DeviceTuners
		for _, base := range bases {
			if ctx.Err() != nil {
				return false
			}
			tuners, err := h.poolTuners(ctx, base)
			if err != nil {
				continue
			}
			for i := range tuners {
				if skipped[skipKey(base, tuners[i].Index)] {
					tuners[i].Guide = "held"
				}
			}
			devices = append(devices, DeviceTuners{Host: hostOf(base), Base: base, Tuners: tuners})
		}
		picked, tuner, ok := PickTuner(devices, nil, nil, need)
		if !ok {
			return false
		}
		base := picked
		if !strings.Contains(base, "://") {
			base = "http://" + picked
		}
		skipped[skipKey(base, tuner)] = true
		root := h.streamRootFor(ctx, base, guide)
		body, err := openReplacement(ctx, root, tuner, guide, m.freq)
		if err != nil || body == nil || ctx.Err() != nil {
			if body != nil {
				_ = body.Close()
			}
			continue
		}
		if h.commitMove(ctx, m, base, tuner, body) {
			return true
		}
		_ = body.Close()
		releaseTuner(hostOf(base), tuner)
		return false
	}
	return false
}

func skipKey(base string, tuner int) string {
	return base + "#" + strconv.Itoa(tuner)
}

// commitMove publishes a replacement stream. The viewer may have left while
// the new device was locking, and that tuner must not stay held.
func (h *Hub) commitMove(ctx context.Context, m *mux, base string, tuner int, body io.ReadCloser) bool {
	id := h.deviceID(ctx, base)
	h.mu.Lock()
	if ctx.Err() != nil || h.muxes[m.freq] != m {
		h.mu.Unlock()
		return false
	}
	oldHost, oldTuner, oldBody := m.host, m.tuner, m.body
	m.host = hostOf(base)
	m.base = base
	m.tuner = tuner
	m.body = body
	if id != "" {
		m.device = id
		for _, f := range m.feeds {
			if f == nil {
				continue
			}
			f.channel.DeviceID = id
			f.channel.BaseURL = base
		}
	}
	h.mu.Unlock()
	if oldBody != nil && oldBody != body {
		_ = oldBody.Close()
	}
	releaseTuner(oldHost, oldTuner)
	slog.Info(fmt.Sprintf("mux %d moved to %s tuner %d", m.freq, m.host, tuner))
	return true
}

func (h *Hub) muxChannel(m *mux) (guide, device string, need Need) {
	h.mu.Lock()
	defer h.mu.Unlock()
	device = m.device
	for number, f := range m.feeds {
		if f == nil {
			continue
		}
		guide = number
		if guide == "" {
			guide = f.channel.GuideNumber
		}
		if f.channel.DeviceID != "" {
			device = f.channel.DeviceID
		}
		need = NeedFor(f.channel.VideoCodec, f.channel.AudioCodec, f.channel.ATSC3)
		if guide != "" {
			return guide, device, need
		}
	}
	return "", device, Need{}
}

func (h *Hub) poolTuners(ctx context.Context, base string) ([]Tuner, error) {
	raw, err := fetchTunerStatus(ctx, base)
	if err != nil {
		return nil, err
	}
	out := make([]Tuner, 0, len(raw))
	for i, row := range raw {
		out = append(out, Tuner{
			Index: i, Guide: row.VctNumber, Name: row.VctName, Target: row.TargetIP,
			Strength: row.SignalStrengthPercent, Quality: row.SignalQualityPercent, Symbol: row.SymbolQualityPercent,
		})
	}
	h.markATSC3(ctx, base, out)
	return out, nil
}

func (h *Hub) markATSC3(ctx context.Context, base string, tuners []Tuner) {
	if h.Store == nil || len(tuners) == 0 {
		return
	}
	devs, err := h.Store.Devices(ctx)
	if err != nil {
		return
	}
	model := modelFor(devs, base)
	if model == "" {
		return
	}
	markATSC3(tuners, atsc3Tuners(model))
}

func (h *Hub) deviceID(ctx context.Context, base string) string {
	if h.Store == nil {
		return ""
	}
	devs, err := h.Store.Devices(ctx)
	if err != nil {
		return ""
	}
	for _, d := range devs {
		if sameBase(d.BaseURL, base) {
			return d.DeviceID
		}
	}
	return ""
}

func modelFor(devs []store.Device, base string) string {
	host := hostOf(base)
	fuzzy := ""
	fuzzyN := 0
	for _, d := range devs {
		if sameBase(d.BaseURL, base) {
			return d.ModelNumber
		}
		if host != "" && hostOf(d.BaseURL) == host {
			fuzzy = d.ModelNumber
			fuzzyN++
		}
	}
	if fuzzyN == 1 {
		return fuzzy
	}
	return ""
}

func sameBase(a, b string) bool {
	return canonBase(a) == canonBase(b) && canonBase(a) != ""
}

func canonBase(s string) string {
	s = strings.TrimRight(strings.TrimSpace(s), "/")
	if s == "" {
		return ""
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return strings.ToLower(s)
	}
	return strings.ToLower(u.Scheme + "://" + u.Host)
}

type tunerStatus struct {
	Resource              string
	VctNumber             string
	VctName               string
	TargetIP              string
	SignalStrengthPercent int
	SignalQualityPercent  int
	SymbolQualityPercent  int
}

func fetchTunerStatus(ctx context.Context, host string) ([]tunerStatus, error) {
	var raw []tunerStatus
	base := host
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/status.json", nil)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// fullMuxHz is the smallest RF frequency a handoff treats as a whole mux.
// A test mux below this opens the one channel instead.
const fullMuxHz = 50_000_000

// openReplacement tunes the chosen tuner. A real frequency opens the full mux
// (/tunerN/chFREQ). Anything else opens that one channel. The caller releases it.
func openReplacement(ctx context.Context, root string, tuner int, guide string, freq int) (io.ReadCloser, error) {
	root = strings.TrimRight(root, "/")
	if freq >= fullMuxHz {
		body, err := openKept(ctx, fmt.Sprintf("%s/tuner%d/ch%d", root, tuner, freq))
		if err == nil {
			return body, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return openKept(ctx, fmt.Sprintf("%s/tuner%d/v%s", root, tuner, url.PathEscape(guide)))
}

// openKept starts a tuner stream. ctx only bounds the wait for a response.
// A body that arrives stays open until the caller closes it, so the move
// budget does not cut the new stream.
func openKept(ctx context.Context, u string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reqCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	halt := context.AfterFunc(ctx, stop)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, u, nil)
	if err != nil {
		halt()
		stop()
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if !halt() {
		stop()
		if res != nil {
			res.Body.Close()
		}
		if err == nil {
			err = ctx.Err()
		}
		return nil, err
	}
	if err != nil {
		stop()
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		stop()
		return nil, fmt.Errorf("tuner returned %s", res.Status)
	}
	return &keptBody{ReadCloser: res.Body, stop: stop}, nil
}

// keptBody is a tuner stream whose request context outlives the move budget.
type keptBody struct {
	io.ReadCloser
	stop func()
	once sync.Once
}

func (b *keptBody) Close() error {
	if b.stop != nil {
		b.once.Do(b.stop)
	}
	return b.ReadCloser.Close()
}

func releaseTuner(host string, tuner int) {
	if host == "" || tuner < 0 {
		return
	}
	_, _ = hdhr.Control{Addr: controlAddr(host)}.Set(fmt.Sprintf("/tuner%d/channel", tuner), "none")
}

func firstFree(tuners []Tuner, used, reserved map[int]bool) (int, bool) {
	for _, t := range tuners {
		if used[t.Index] || reserved[t.Index] || t.Target != "" || t.Guide != "" {
			continue
		}
		return t.Index, true
	}
	return 0, false
}

func openStream(u, userAgent, referrer string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	if referrer != "" {
		req.Header.Set("Referer", referrer)
	}
	res, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return nil, fmt.Errorf("stream returned %s", res.Status)
	}
	return res, nil
}

func probe(host string, tuner int, guide string) (int, []hdhr.Program, error) {
	c := hdhr.Control{Addr: controlAddr(host)}
	name := fmt.Sprintf("/tuner%d/vchannel", tuner)
	if _, err := c.Set(name, guide); err != nil {
		return 0, nil, err
	}
	var status string
	var err error
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		status, err = c.Get(fmt.Sprintf("/tuner%d/status", tuner))
		if err == nil && hdhr.FrequencyHz(status) > 0 && strings.Contains(status, "lock=8vsb") {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	freq := hdhr.FrequencyHz(status)
	if freq == 0 {
		_, _ = c.Set(fmt.Sprintf("/tuner%d/channel", tuner), "none")
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("tuner %d did not lock %s", tuner, guide)
	}
	info := ""
	infoDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(infoDeadline) {
		info, err = c.Get(fmt.Sprintf("/tuner%d/streaminfo", tuner))
		if err == nil && strings.Contains(info, guide) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		_, _ = c.Set(fmt.Sprintf("/tuner%d/channel", tuner), "none")
		return 0, nil, err
	}
	programs := hdhr.ParseStreamInfo(info)
	if programFor(programs, guide) == 0 {
		_, _ = c.Set(fmt.Sprintf("/tuner%d/channel", tuner), "none")
		return 0, nil, fmt.Errorf("tuner %d locked %s but did not list that channel", tuner, guide)
	}
	return freq, programs, nil
}

func controlAddr(host string) string {
	if p := os.Getenv("HDHR_CONTROL_PORT"); p != "" {
		return net.JoinHostPort(host, p)
	}
	return host
}

func streamRoot(ch store.SourceChannel) string {
	if u, err := url.Parse(ch.StreamURL); err == nil && u.Host != "" {
		return u.Scheme + "://" + u.Host
	}
	return "http://" + hostOf(ch.BaseURL) + ":5004"
}

// streamRootFor is where a device actually streams. The discovery origin is
// often port 80, and the broadcast is on port 5004. A test fake uses one port.
func (h *Hub) streamRootFor(ctx context.Context, base, guide string) string {
	if h != nil && h.Store != nil && guide != "" {
		if raw, err := h.Store.ChannelStreamURL(ctx, base, guide); err == nil {
			if origin := originOf(raw); origin != "" {
				return origin
			}
		}
	}
	return streamOrigin(base)
}

func originOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	return u.Scheme + "://" + u.Host
}

func streamOrigin(base string) string {
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" {
		host := hostOf(base)
		if host == "" {
			return strings.TrimRight(base, "/")
		}
		return "http://" + host + ":5004"
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	if p := u.Port(); p != "" && p != "80" {
		return u.Scheme + "://" + u.Host
	}
	return u.Scheme + "://" + u.Hostname() + ":5004"
}

func openMux(root string, tuner, freq int) (io.ReadCloser, error) {
	u := fmt.Sprintf("%s/tuner%d/ch%d", strings.TrimRight(root, "/"), tuner, freq)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	res, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		res.Body.Close()
		return nil, fmt.Errorf("tuner returned %s: %s", res.Status, strings.TrimSpace(string(b)))
	}
	return res.Body, nil
}

func (m *mux) detach(sub *pipeSub) {
	if m == nil || sub == nil {
		return
	}
	m.pipeMu.Lock()
	for i, s := range m.pipes {
		if s == sub {
			m.pipes = append(m.pipes[:i], m.pipes[i+1:]...)
			break
		}
	}
	m.pipeMu.Unlock()
	sub.stop()
}

func (m *mux) snapshot() []*pipeSub {
	m.pipeMu.Lock()
	defer m.pipeMu.Unlock()
	return append([]*pipeSub(nil), m.pipes...)
}

func programFor(programs []hdhr.Program, guide string) int {
	for _, p := range programs {
		if p.GuideNumber == guide {
			return p.Number
		}
	}
	return 0
}

func hostOf(base string) string {
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" {
		return strings.TrimPrefix(strings.TrimPrefix(base, "https://"), "http://")
	}
	return u.Hostname()
}

func sanitize(name string) string {
	out := strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			return '-'
		}
		return r
	}, name)
	out = strings.TrimSpace(out)
	if out == "" {
		return "channel"
	}
	return out
}

func muxOf(h *Hub, f *feed) *mux {
	return h.muxes[f.channel.FrequencyHz]
}
