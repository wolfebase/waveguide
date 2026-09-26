package realtime

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func fixedRooms(at time.Time) (*Rooms, *time.Time) {
	now := at
	r := NewRooms()
	r.now = func() time.Time { return now }
	return r, &now
}

func TestFollowRoomTracksLiveAndRefusesControls(t *testing.T) {
	start := time.Date(2026, 9, 22, 20, 0, 0, 0, time.UTC)
	r, now := fixedRooms(start)
	st := r.Join("channel:4", 4, 0)
	if st.Mode != "follow" || st.Members != 1 {
		t.Fatalf("channel rooms follow live: %+v", st)
	}
	want := unixMS(start.Add(-10 * time.Second))
	if got := st.Target(unixMS(start)); got != want {
		t.Fatalf("target should sit 10s behind real time, got %v want %v", got, want)
	}
	*now = start.Add(time.Minute)
	st, _ = r.State("channel:4")
	if got := st.Target(unixMS(*now)); got != want+60000 {
		t.Fatalf("a playing room advances in real time, got %v", got)
	}
	if _, err := r.Apply("channel:4", Command{Action: "pause"}); err != ErrFollowRoom {
		t.Fatalf("follow rooms refuse pause, got %v", err)
	}
}

func TestFreshRoomStartsOnTheFirstFrame(t *testing.T) {
	start := time.Date(2026, 9, 26, 3, 0, 0, 0, time.UTC)
	r, _ := fixedRooms(start)
	first := unixMS(start.Add(-3500 * time.Millisecond))
	st := r.Join("channel:4", 4, first)
	if st.AnchorMedia != first || st.Members != 1 {
		t.Fatalf("a fresh room starts on the first frame: %+v", st)
	}
	next := r.Join("channel:4", 4, unixMS(start))
	if next.AnchorMedia != first || next.Members != 2 {
		t.Fatalf("the next screen keeps that frame: %+v", next)
	}
	deep := r.Join("channel:9", 9, unixMS(start.Add(-30*time.Second)))
	want := unixMS(start.Add(-10 * time.Second))
	if deep.AnchorMedia != want {
		t.Fatalf("a deep buffer stays at the latency target, got %v want %v", deep.AnchorMedia, want)
	}
}

func TestFollowRoomSettlesOnceTheBufferCoversTheLatency(t *testing.T) {
	start := time.Date(2026, 9, 26, 21, 0, 0, 0, time.UTC)
	r, now := fixedRooms(start)
	first := unixMS(start.Add(-3500 * time.Millisecond))
	st := r.Join("channel:4", 4, first)
	if st.AnchorMedia != first {
		t.Fatalf("a fresh room starts on the first frame: %+v", st)
	}
	if moved := r.Settle(4, first); len(moved) != 0 {
		t.Fatalf("a fresh first frame must stay, got %+v", moved)
	}
	*now = start.Add(20 * time.Second)
	recent := unixMS(now.Add(-3 * time.Second))
	if moved := r.Settle(4, recent); len(moved) != 0 {
		t.Fatalf("a first frame newer than the target must stay, got %+v", moved)
	}
	moved := r.Settle(4, first)
	if len(moved) != 1 {
		t.Fatalf("expected one settle, got %+v", moved)
	}
	want := unixMS(now.Add(-10 * time.Second))
	if math.Abs(moved[0].Target(unixMS(*now))-want) > 1 || moved[0].Rate != 1 {
		t.Fatalf("settled target %v want %v (%+v)", moved[0].Target(unixMS(*now)), want, moved[0])
	}
	held := moved[0].Version
	if again := r.Settle(4, first); len(again) != 0 {
		t.Fatalf("a second settle moved the room: %+v", again)
	}
	st, _ = r.State("channel:4")
	if st.Version != held || math.Abs(st.Target(unixMS(*now))-want) > 1 {
		t.Fatalf("target drifted after the second settle: %+v", st)
	}
	g := r.Join("group:den", 4, first)
	*now = start.Add(40 * time.Second)
	if moved := r.Settle(4, first); len(moved) != 0 {
		t.Fatalf("a group room must stay: %+v (group %+v)", moved, g)
	}
	if st, _ = r.State("group:den"); st.AnchorMedia != g.AnchorMedia || st.Version != g.Version {
		t.Fatalf("group anchor changed: %+v", st)
	}
}

func TestMultiviewRoomSharesOneTarget(t *testing.T) {
	start := time.Date(2026, 9, 22, 20, 0, 0, 0, time.UTC)
	r, _ := fixedRooms(start)
	a := r.Join("multiview:games", 0, 0)
	b := r.Join("multiview:games", 0, 0)
	if a.Mode != "group" || b.Members != 2 || a.AnchorMedia != b.AnchorMedia {
		t.Fatalf("tiles share one live target: %+v %+v", a, b)
	}
	st, err := r.Apply("multiview:games", Command{Action: "pause"})
	if err != nil || st.Rate != 0 {
		t.Fatalf("pause moves every tile: %+v %v", st, err)
	}
}

func TestGroupRoomPauseSeekLive(t *testing.T) {
	start := time.Date(2026, 9, 22, 20, 0, 0, 0, time.UTC)
	r, now := fixedRooms(start)
	r.Join("group:den", 9, 0)
	r.Join("group:den", 9, 0)
	*now = start.Add(30 * time.Second)
	st, err := r.Apply("group:den", Command{Action: "pause"})
	if err != nil || st.Rate != 0 || st.Members != 2 {
		t.Fatalf("pause: %+v %v", st, err)
	}
	paused := st.Target(unixMS(*now))
	*now = start.Add(90 * time.Second)
	st, _ = r.State("group:den")
	if st.Target(unixMS(*now)) != paused {
		t.Fatal("a paused room holds its frame")
	}
	st, _ = r.Apply("group:den", Command{Action: "play"})
	*now = start.Add(100 * time.Second)
	if got := st.Target(unixMS(*now)); math.Abs(got-(paused+10000)) > 0.001 {
		t.Fatalf("play resumes from the paused frame, got %v want %v", got, paused+10000)
	}
	st, _ = r.Apply("group:den", Command{Action: "seek", MediaTime: unixMS(*now) + 60000})
	if st.AnchorMedia > unixMS(now.Add(-6*time.Second)) {
		t.Fatal("seeking past the live edge is clamped")
	}
	st, _ = r.Apply("group:den", Command{Action: "live"})
	if st.AnchorMedia != unixMS(now.Add(-10*time.Second)) || st.Rate != 1 {
		t.Fatalf("live jumps to the latency target: %+v", st)
	}
	r.Leave("group:den")
	r.Leave("group:den")
	if _, ok := r.State("group:den"); ok {
		t.Fatal("empty rooms are forgotten")
	}
}

func TestSocketClockAndRoomBroadcast(t *testing.T) {
	bus := NewBus()
	srv := httptest.NewServer(bus)
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dial := func() *websocket.Conn {
		c, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	read := func(c *websocket.Conn, kind string) json.RawMessage {
		for {
			_, raw, err := c.Read(ctx)
			if err != nil {
				t.Fatalf("waiting for %s: %v", kind, err)
			}
			var m Message
			_ = json.Unmarshal(raw, &m)
			if m.Type == kind {
				return m.Data
			}
		}
	}
	send := func(c *websocket.Conn, kind string, v any) {
		data, _ := json.Marshal(v)
		raw, _ := json.Marshal(Message{Type: kind, Data: data})
		if err := c.Write(ctx, websocket.MessageText, raw); err != nil {
			t.Fatal(err)
		}
	}
	tv, phone := dial(), dial()
	defer tv.CloseNow()
	defer phone.CloseNow()
	read(tv, "hello")
	read(phone, "hello")

	send(tv, "clock", map[string]float64{"t0": 123})
	var clock struct{ T0, T1 float64 }
	_ = json.Unmarshal(read(tv, "clock"), &clock)
	if clock.T0 != 123 || clock.T1 <= 0 {
		t.Fatalf("clock echo: %+v", clock)
	}

	send(tv, "sync.join", map[string]any{"room": "group:den", "channelId": 9})
	read(tv, "sync.state")
	send(phone, "sync.join", map[string]any{"room": "group:den", "channelId": 9})
	var st RoomState
	_ = json.Unmarshal(read(phone, "sync.state"), &st)
	if st.Members != 2 {
		t.Fatalf("second member: %+v", st)
	}
	send(phone, "sync.command", map[string]any{"room": "group:den", "action": "pause"})
	for {
		_ = json.Unmarshal(read(tv, "sync.state"), &st)
		if st.Rate == 0 {
			break
		}
	}
	bus.Publish("recording.started", map[string]int{"id": 5})
	if !strings.Contains(string(read(phone, "recording.started")), `"id":5`) {
		t.Fatal("events reach every client")
	}
}

func TestFreshJoinUsesTheFirstFrame(t *testing.T) {
	bus := NewBus()
	start := time.Date(2026, 9, 26, 3, 0, 0, 0, time.UTC)
	bus.SetClock(func() time.Time { return start })
	first := unixMS(start.Add(-3500 * time.Millisecond))
	bus.MediaStart = func(channelID int64) (float64, bool) {
		if channelID == 4 {
			return first, true
		}
		return 0, false
	}
	srv := httptest.NewServer(bus)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	data, _ := json.Marshal(map[string]any{"room": "channel:4", "channelId": 4})
	raw, _ := json.Marshal(Message{Type: "sync.join", Data: data})
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
	var st RoomState
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var m Message
		_ = json.Unmarshal(msg, &m)
		if m.Type != "sync.state" {
			continue
		}
		_ = json.Unmarshal(m.Data, &st)
		if st.AnchorMedia != first {
			t.Fatalf("join should anchor on the first frame, got %v", st.AnchorMedia)
		}
		return
	}
	t.Fatal("no room state")
}

func TestHereAnnouncesAScreen(t *testing.T) {
	bus := NewBus()
	srv := httptest.NewServer(bus)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	raw, _ := json.Marshal(Message{Type: "here", Data: json.RawMessage(`{"name":"Broadwave Staging TV","kind":"appletv"}`)})
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var screens []Presence
	for time.Now().Before(deadline) {
		screens = bus.Screens()
		if len(screens) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(screens) != 1 || screens[0].Kind != "appletv" || screens[0].Name != "Broadwave Staging TV" {
		t.Fatalf("%+v", screens)
	}
	conn.Close(websocket.StatusNormalClosure, "")
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(bus.Screens()) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("screen stayed after disconnect: %+v", bus.Screens())
}
