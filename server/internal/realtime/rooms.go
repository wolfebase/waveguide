package realtime

import (
	"errors"
	"strings"
	"sync"
	"time"
)

// Latency targets: how far behind real time a sync room plays. The shared
// timeline is broadcast wall time, so the same target works for every rendition.
var latencies = map[string]time.Duration{
	"lowest":   6 * time.Second,
	"balanced": 10 * time.Second,
	"stable":   20 * time.Second,
}

// RoomState is the whole sync contract. At server time T the room plays media
// time AnchorMedia + (T - AnchorServer) * Rate. Times are Unix milliseconds;
// media time is the program date-time of the frame on screen.
type RoomState struct {
	Room         string  `json:"room"`
	ChannelID    int64   `json:"channelId"`
	Mode         string  `json:"mode"` // follow: shared playback, own controls; group: controls move everyone
	AnchorServer float64 `json:"anchorServer"`
	AnchorMedia  float64 `json:"anchorMedia"`
	Rate         float64 `json:"rate"`
	Latency      string  `json:"latency"`
	Version      int     `json:"version"`
	Members      int     `json:"members"`
}

// Target is the media time the room shows at server time now (Unix ms).
func (s RoomState) Target(now float64) float64 {
	return s.AnchorMedia + (now-s.AnchorServer)*s.Rate
}

type Command struct {
	Action    string  `json:"action"` // play, pause, seek, live, latency
	MediaTime float64 `json:"mediaTime,omitempty"`
	Latency   string  `json:"latency,omitempty"`
}

var ErrFollowRoom = errors.New("only group rooms take playback commands")

type Rooms struct {
	mu    sync.Mutex
	rooms map[string]*RoomState
	now   func() time.Time
}

func NewRooms() *Rooms {
	return &Rooms{rooms: map[string]*RoomState{}, now: time.Now}
}

func unixMS(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e6
}

func liveAnchor(now time.Time, latency string) float64 {
	d, ok := latencies[latency]
	if !ok {
		d = latencies["balanced"]
	}
	return unixMS(now.Add(-d))
}

// Join adds a member, creating the room at the live target if it is new.
// Channel rooms ("channel:ID") follow live. Group rooms ("group:CODE") and
// multiview rooms ("multiview:ID") share controls, so pause hits every tile.
// earliest is the first program time the channel can play, in Unix ms. Zero
// means unknown. A fresh tune's first frame is newer than the latency target,
// and aiming past it makes the player pause until the wall clock catches up.
// The next member keeps that anchor. lowest, balanced, and stable still apply
// once the buffer is deep enough to hold them.
func (r *Rooms) Join(room string, channelID int64, earliest float64) RoomState {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.rooms[room]
	if st == nil {
		now := r.now()
		mode := "follow"
		if strings.HasPrefix(room, "group:") || strings.HasPrefix(room, "multiview:") {
			mode = "group"
		}
		media := liveAnchor(now, "balanced")
		if earliest > media {
			media = earliest
		}
		st = &RoomState{
			Room: room, ChannelID: channelID, Mode: mode, Latency: "balanced", Rate: 1,
			AnchorServer: unixMS(now), AnchorMedia: media, Version: 1,
		}
		r.rooms[room] = st
	}
	st.Members++
	return *st
}

// Settle moves follow rooms on this channel from a first-frame anchor back to
// the latency target once earliest (Unix ms) is old enough to play there.
// A fresh tune, a room already on its target, a paused room, and a group room
// stay put. Each changed state is returned so members can be told; a second
// call is empty.
func (r *Rooms) Settle(channelID int64, earliest float64) []RoomState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if earliest <= 0 {
		return nil
	}
	var changed []RoomState
	for _, st := range r.rooms {
		if st.ChannelID != channelID || st.Mode != "follow" || st.Rate != 1 {
			continue
		}
		now := r.now()
		nowMS := unixMS(now)
		target := liveAnchor(now, st.Latency)
		if earliest > target {
			continue
		}
		// Already at the target, or further behind it. Never pull a room toward live.
		if st.Target(nowMS) <= target+500 {
			continue
		}
		st.AnchorServer, st.AnchorMedia, st.Rate = nowMS, target, 1
		st.Version++
		changed = append(changed, *st)
	}
	return changed
}

// Leave drops a member; an empty room is forgotten.
func (r *Rooms) Leave(room string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.rooms[room]
	if st == nil {
		return
	}
	st.Members--
	if st.Members <= 0 {
		delete(r.rooms, room)
	}
}

func (r *Rooms) State(room string) (RoomState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.rooms[room]
	if st == nil {
		return RoomState{}, false
	}
	return *st, true
}

// Apply runs a playback command and returns the new state for every member.
func (r *Rooms) Apply(room string, c Command) (RoomState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.rooms[room]
	if st == nil {
		return RoomState{}, errors.New("not in that room")
	}
	now := r.now()
	nowMS := unixMS(now)
	if c.Action == "latency" {
		if _, ok := latencies[c.Latency]; !ok {
			return RoomState{}, errors.New("latency is lowest, balanced, or stable")
		}
		st.Latency = c.Latency
		if st.Mode == "follow" {
			st.AnchorServer, st.AnchorMedia, st.Rate = nowMS, liveAnchor(now, st.Latency), 1
		}
		st.Version++
		return *st, nil
	}
	if st.Mode != "group" {
		return RoomState{}, ErrFollowRoom
	}
	current := st.Target(nowMS)
	switch c.Action {
	case "pause":
		st.AnchorServer, st.AnchorMedia, st.Rate = nowMS, current, 0
	case "play":
		st.AnchorServer, st.AnchorMedia, st.Rate = nowMS, current, 1
	case "seek":
		limit := liveAnchor(now, "lowest")
		media := c.MediaTime
		if media > limit {
			media = limit
		}
		st.AnchorServer, st.AnchorMedia = nowMS, media
	case "live":
		st.AnchorServer, st.AnchorMedia, st.Rate = nowMS, liveAnchor(now, st.Latency), 1
	default:
		return RoomState{}, errors.New("unknown action")
	}
	st.Version++
	return *st, nil
}
