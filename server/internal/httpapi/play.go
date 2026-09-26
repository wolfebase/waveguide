package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"broadwave/internal/dvr"
	"broadwave/internal/guide"
	"broadwave/internal/live"
	"broadwave/internal/store"
)

func (s *Server) watch(w http.ResponseWriter, r *http.Request) {
	if s.Hub == nil {
		httpError(w, "Live TV is not set up on this server.", http.StatusServiceUnavailable)
		return
	}
	var body struct {
		ChannelID   int64      `json:"channelId"`
		Caps        *live.Caps `json:"caps"`
		Prefs       live.Prefs `json:"prefs"`
		Rendition   string     `json:"rendition"`
		Profile     string     `json:"profile"`
		Audio       string     `json:"audio"`
		Picture     string     `json:"pictureMode"`
		ConfirmLive bool       `json:"confirmLive"`
	}
	if err := decodeJSON(r, &body); err != nil {
		httpError(w, "invalid json", http.StatusBadRequest)
		return
	}
	src, err := s.Hub.SourceOf(r.Context(), body.ChannelID)
	if err != nil {
		writeError(w, err)
		return
	}
	var decision live.Decision
	if forced, ok := live.ParseRenditionKey(body.Rendition); ok {
		decision = live.Decision{Rendition: forced, Reason: "Chosen in the player"}
	} else {
		caps, prefs := live.LegacyCaps(body.Profile, body.Audio, body.Picture)
		if body.Caps != nil {
			caps, prefs = *body.Caps, body.Prefs
		}
		if prefs.Picture == "" {
			_, prefs.Picture, _ = s.playbackChoice(r.Context(), 0, "")
		}
		decision = live.DecideFor(src, caps, prefs, s.Hub.Encoder, s.Hub.Host)
	}
	if !body.ConfirmLive {
		if msg := s.liveWarning(r.Context(), body.ChannelID); msg != "" {
			apiError(w, http.StatusConflict, "recording_soon", msg, nil)
			return
		}
	}
	session, err := s.Hub.Watch(r.Context(), body.ChannelID, decision.Rendition)
	if err != nil {
		writeError(w, err)
		return
	}
	session.Stream.Reason = decision.Reason
	if session.Rendition != decision.Rendition.Key() && session.Stream.Video != "" && session.Stream.Video != "copy" {
		session.Stream.Reason = "Playing the " + session.Stream.Video + "p picture already running."
	}
	waitServable(s.Hub, session.ChannelID, session.Rendition, 12*time.Second)
	if fresh, ok := s.Hub.Session(session.ChannelID, session.Rendition); ok {
		reason := session.Stream.Reason
		fresh.Tuners = session.Tuners
		session = fresh
		session.Stream.Reason = reason
	}
	// Tuner status is a separate request. Reading it here holds the hub lock
	// after the first segment already exists, so the player cannot start.
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) release(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, "invalid channel", http.StatusBadRequest)
		return
	}
	var body struct {
		Rendition string `json:"rendition"`
	}
	_ = decodeJSON(r, &body)
	if s.Hub != nil {
		s.Hub.Release(id, body.Rendition)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) tuners(w http.ResponseWriter, r *http.Request) {
	if s.Hub == nil {
		writeJSON(w, http.StatusOK, map[string]any{"tuners": []live.Tuner{}})
		return
	}
	list, err := s.Hub.Tuners(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if list == nil {
		list = []live.Tuner{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tuners": list, "encoder": s.Hub.Encoder})
}

func (s *Server) startRecording(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ChannelID int64  `json:"channelId"`
		Minutes   int    `json:"minutes"`
		Title     string `json:"title"`
	}
	if err := decodeJSON(r, &body); err != nil {
		httpError(w, "invalid json", http.StatusBadRequest)
		return
	}
	meta := store.Recording{ChannelID: body.ChannelID, Title: strings.TrimSpace(body.Title)}
	minutes := body.Minutes
	if air, ok := s.listingFor(r.Context(), body.ChannelID, meta.Title); ok {
		meta.Title = air.Title
		meta.Subtitle = air.Subtitle
		meta.Description = air.Description
		meta.Category = air.Category
		meta.ProgramID = air.ProgramID
		meta.GameID = air.GameID
		pad := 2 + int(dvr.SportsTail(air)/time.Minute)
		left := int(time.Until(air.End.Add(time.Duration(pad)*time.Minute)).Minutes()) + 1
		if left > minutes {
			minutes = left
		}
	}
	rec, err := s.Hub.RecordMeta(r.Context(), minutes, meta)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) stopRecording(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, "invalid recording", http.StatusBadRequest)
		return
	}
	s.Hub.StopRecord(id)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) recordings(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.Recordings(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if list == nil {
		list = []store.Recording{}
	}
	for i := range list {
		if info, err := os.Stat(list[i].Path); err == nil {
			list[i].Bytes = info.Size()
		}
		if pos, err := s.Store.Progress(r.Context(), list[i].ID); err == nil && pos > 0 {
			list[i].Position = pos
		}
		if list[i].Duration == 0 && list[i].Status != "recording" && list[i].Path != "" && s.Hub != nil {
			if tool := live.FFProbePath(s.Hub.FFmpeg); tool != "" {
				seconds, err := live.ProbeDuration(tool, list[i].Path)
				if err != nil || seconds <= 0 {
					_ = s.Store.SetDuration(r.Context(), list[i].ID, -1)
				} else {
					_ = s.Store.SetDuration(r.Context(), list[i].ID, seconds)
					list[i].Duration = seconds
				}
			}
		}
		if list[i].Duration < 0 {
			list[i].Duration = 0
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"recordings": list})
}

func (s *Server) deleteRecording(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, "invalid recording", http.StatusBadRequest)
		return
	}
	rec, err := s.Store.Recording(r.Context(), id)
	if err != nil {
		httpError(w, "recording not found", http.StatusNotFound)
		return
	}
	if rec.Status == "recording" {
		httpError(w, "stop the recording before deleting it", http.StatusConflict)
		return
	}
	if s.Hub != nil {
		s.Hub.StopRecord(id)
		removeRecordingFiles(s.Hub.Dir, rec)
	}
	if err := s.Store.DeleteRecording(r.Context(), id); err != nil {
		writeError(w, err)
		return
	}
	_ = s.Store.AddEvent(r.Context(), "delete", "Deleted "+rec.Title)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.Events(r.Context(), 40)
	if err != nil {
		writeError(w, err)
		return
	}
	if list == nil {
		list = []store.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": list})
}

func removeRecordingFiles(workDir string, rec store.Recording) {
	root := filepath.Join(workDir, "recordings")
	removeInside(root, rec.Path)
	base := strings.TrimSuffix(rec.Path, filepath.Ext(rec.Path))
	removeInside(root, base+".edl")
	removeInside(root, base+".json")
	_ = os.Remove(filepath.Join(workDir, "posters", strconv.FormatInt(rec.ID, 10)+".jpg"))
	_ = os.RemoveAll(filepath.Join(workDir, "file", strconv.FormatInt(rec.ID, 10)))
}

func removeInside(root, path string) {
	if path == "" {
		return
	}
	clean := filepath.Clean(path)
	rel, err := filepath.Rel(filepath.Clean(root), clean)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return
	}
	_ = os.Remove(clean)
}

func (s *Server) airings(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	from := now.Add(-30 * time.Minute)
	to := now.Add(48 * time.Hour)
	if raw := r.URL.Query().Get("from"); raw != "" {
		parsed, err := parseGuideTime(raw)
		if err != nil {
			httpError(w, "from needs to be a time", http.StatusBadRequest)
			return
		}
		from = parsed
	}
	if raw := r.URL.Query().Get("to"); raw != "" {
		parsed, err := parseGuideTime(raw)
		if err != nil {
			httpError(w, "to needs to be a time", http.StatusBadRequest)
			return
		}
		to = parsed
	} else if raw := r.URL.Query().Get("hours"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 168 {
			to = now.Add(time.Duration(n) * time.Hour)
		}
	}
	if !to.After(from) {
		httpError(w, "The guide window is backwards.", http.StatusBadRequest)
		return
	}
	list, err := s.Store.Airings(r.Context(), from, to)
	if err != nil {
		writeError(w, err)
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("channels")); raw != "" {
		want := map[int64]bool{}
		for _, part := range strings.Split(raw, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if err != nil {
				httpError(w, "channels needs to be a list of ids", http.StatusBadRequest)
				return
			}
			want[id] = true
		}
		filtered := make([]store.Airing, 0, len(list))
		for _, row := range list {
			if want[row.ChannelID] {
				filtered = append(filtered, row)
			}
		}
		list = filtered
	}
	if list == nil {
		list = []store.Airing{}
	}
	writeCachedJSON(w, r, http.StatusOK, map[string]any{"airings": list})
}

func parseGuideTime(raw string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}

func (s *Server) refreshGuide(w http.ResponseWriter, r *http.Request) {
	_, _, lastManual, err := s.Store.GuideSchedule(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if ok, retry := guide.ManualAllowed(lastManual, time.Now()); !ok {
		mins := int(time.Until(retry).Minutes()) + 1
		if mins < 1 {
			mins = 1
		}
		apiError(w, http.StatusTooManyRequests, "guide_rate_limited", fmt.Sprintf("Listings were just refreshed. Try again in %d minutes.", mins), map[string]any{"retryAt": retry.UTC()})
		return
	}
	if err := s.Store.SetManualGuidePull(r.Context(), time.Now()); err != nil {
		writeError(w, err)
		return
	}
	pull := s.RefreshGuide
	if s.GuidePull != nil {
		pull = s.GuidePull
	}
	n, err := pull(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"airings": n})
}

// GuideDelay is how long the automatic refresh should wait. Zero means pull now.
func (s *Server) GuideDelay(now time.Time) time.Duration {
	_, next, _, err := s.Store.GuideSchedule(context.Background())
	if err != nil {
		return 0
	}
	return guide.Delay(next, now)
}

// DeferGuide schedules another attempt after a failed pull without counting it as a success.
func (s *Server) DeferGuide(ctx context.Context, after time.Duration) {
	_ = s.Store.SetNextGuidePull(ctx, time.Now().Add(after))
}

func (s *Server) RefreshGuide(ctx context.Context) (int, error) {
	devices, err := s.Store.Devices(ctx)
	if err != nil {
		return 0, err
	}
	if len(devices) == 0 {
		return 0, errors.New("no tuner")
	}
	raw, err := guide.Pull(ctx, s.HDHR, devices[0].BaseURL)
	if err != nil {
		return 0, err
	}
	channels, err := s.Store.Channels(ctx, false)
	if err != nil {
		return 0, err
	}
	var antenna []store.Channel
	var ids []int64
	for _, ch := range channels {
		if strings.HasPrefix(ch.DeviceID, "src-") {
			continue
		}
		antenna = append(antenna, ch)
		ids = append(ids, ch.ID)
	}
	rows, art, err := guide.Parse(raw, antenna)
	if err != nil {
		return 0, err
	}
	_ = s.Store.SetChannelArt(ctx, art)
	_ = s.Store.SetNetworks(ctx, guide.Networks(raw, antenna))
	settings, _ := s.Store.Settings(ctx)
	if settings == nil {
		settings = map[string]string{}
	}
	tmdbKey := strings.TrimSpace(settings["tmdbKey"])
	if tmdbKey == "" {
		tmdbKey = strings.TrimSpace(os.Getenv("TMDB_API_KEY"))
	}
	rows = guide.FillImages(ctx, tmdbKey, rows)
	rows = tagGuideSource(rows, "silicondust")
	if err := s.Store.ReplaceAiringsFor(ctx, ids, rows); err != nil {
		return 0, err
	}
	user := strings.TrimSpace(settings["sdUser"])
	pass := settings["sdPassword"]
	lineup := strings.TrimSpace(settings["sdLineup"])
	if user == "" {
		user = strings.TrimSpace(os.Getenv("SD_USERNAME"))
	}
	if pass == "" {
		pass = os.Getenv("SD_PASSWORD")
	}
	if lineup == "" {
		lineup = strings.TrimSpace(os.Getenv("SD_LINEUP"))
	}
	if extra, _, err := guide.SchedulesDirect(ctx, antenna, user, pass, lineup); err == nil && len(extra) > 0 {
		rows = s.fillUnlisted(ctx, rows, tagGuideSource(extra, "schedules-direct"))
	}
	if rawURL := strings.TrimSpace(settings["guideUrl"]); rawURL != "" {
		if body, err := guide.PullURL(ctx, rawURL); err == nil {
			if extra, extraArt, err := guide.Parse(body, antenna); err == nil && len(extra) > 0 {
				_ = s.Store.SetChannelArt(ctx, extraArt)
				_ = s.Store.SetNetworks(ctx, guide.Networks(body, antenna))
				extra = guide.FillImages(ctx, tmdbKey, extra)
				rows = s.fillUnlisted(ctx, rows, tagGuideSource(extra, "xmltv"))
			}
		}
	}
	now := time.Now().UTC()
	span := int64(guide.PullMax - guide.PullMin)
	jitter := time.Duration(rand.Int64N(span + 1))
	next := guide.NextPull(now, jitter)
	if err := s.Store.SetGuideSchedule(ctx, now, next); err != nil {
		return len(rows), err
	}
	listed := map[int64]struct{}{}
	for _, row := range rows {
		listed[row.ChannelID] = struct{}{}
	}
	slog.Info(fmt.Sprintf("guide: source=silicondust-xmltv airings=%d channels=%d next=%s", len(rows), len(listed), next.Format(time.RFC3339)))
	_ = s.Store.AddEvent(ctx, "guide", fmt.Sprintf("Guide updated, %d airings", len(rows)))
	s.LinkGames(ctx)
	return len(rows), nil
}

// fillUnlisted keeps the listings a channel already has and adds rows only for channels that have none.
func (s *Server) fillUnlisted(ctx context.Context, rows, extra []store.Airing) []store.Airing {
	have := map[int64]bool{}
	for _, row := range rows {
		have[row.ChannelID] = true
	}
	var fill []store.Airing
	for _, row := range extra {
		if !have[row.ChannelID] {
			fill = append(fill, row)
		}
	}
	if len(fill) == 0 {
		return rows
	}
	var fillIDs []int64
	seen := map[int64]bool{}
	for _, row := range fill {
		if !seen[row.ChannelID] {
			seen[row.ChannelID] = true
			fillIDs = append(fillIDs, row.ChannelID)
		}
	}
	_ = s.Store.ReplaceAiringsFor(ctx, fillIDs, fill)
	return append(rows, fill...)
}

func (s *Server) schedule(w http.ResponseWriter, r *http.Request) {
	snap, err := s.loadSchedule(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tunerCount": snap.count, "items": snap.items})
}

type scheduleSnap struct {
	items   []dvr.Planned
	passes  []store.Pass
	airings []store.Airing
	count   int
}

func (s *Server) loadSchedule(ctx context.Context) (scheduleSnap, error) {
	now := time.Now()
	end := now.Add(14 * 24 * time.Hour)
	passes, err := s.Store.Passes(ctx)
	if err != nil {
		return scheduleSnap{}, err
	}
	airings, err := s.Store.Airings(ctx, now.Add(-time.Minute), end)
	if err != nil {
		return scheduleSnap{}, err
	}
	count := s.tunerCount(ctx)
	items := dvr.Plan(passes, airings, count, now, end)
	recs, _ := s.Store.Recordings(ctx)
	seen, _ := s.Store.SeenDeleted(ctx)
	skips, _ := s.Store.Skips(ctx)
	items = dvr.ApplyLibrary(items, passes, recs, seen, skips)
	items = dvr.AttachSuggestions(items, passes, airings, count, now, end, s.guideNumbers(ctx))
	if items == nil {
		items = []dvr.Planned{}
	}
	return scheduleSnap{items: items, passes: passes, airings: airings, count: count}, nil
}

func (s *Server) fixSchedule(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PassID              int64       `json:"passId"`
		ChannelID           int64       `json:"channelId"`
		Start               time.Time   `json:"start"`
		SuggestionChannelID int64       `json:"suggestionChannelId"`
		SuggestionStart     time.Time   `json:"suggestionStart"`
		AcknowledgeMisses   bool        `json:"acknowledgeMisses"`
		AcknowledgedStarts  []time.Time `json:"acknowledgedStarts"`
	}
	if err := decodeJSON(r, &body); err != nil {
		httpError(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.PassID == 0 || body.ChannelID == 0 || body.Start.IsZero() || body.SuggestionChannelID == 0 || body.SuggestionStart.IsZero() {
		httpError(w, "A pass and an airing are required.", http.StatusBadRequest)
		return
	}
	snap, err := s.loadSchedule(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	var item *dvr.Planned
	for i := range snap.items {
		if snap.items[i].PassID == body.PassID && snap.items[i].Airing.ChannelID == body.ChannelID && snap.items[i].Airing.Start.Equal(body.Start) {
			item = &snap.items[i]
			break
		}
	}
	if item == nil || !item.Skipped || item.Suggestion == nil {
		httpError(w, "That showing is not waiting on a tuner.", http.StatusConflict)
		return
	}
	if item.Suggestion.ChannelID != body.SuggestionChannelID || !item.Suggestion.Start.Equal(body.SuggestionStart) {
		httpError(w, "That later airing no longer fits.", http.StatusConflict)
		return
	}
	pass, ok := passIn(snap.passes, body.PassID)
	suggestion, sugOK := airingAt(snap.airings, body.SuggestionChannelID, body.SuggestionStart)
	if !ok || !sugOK {
		httpError(w, "That showing is not waiting on a tuner.", http.StatusConflict)
		return
	}
	fix := dvr.PlanFix(pass, item.Airing, suggestion)
	if fix.OneShot != nil && !dvr.HaveOneShot(snap.passes, suggestion) {
		missed := dvr.MissedFrom(snap.airings, *fix.OneShot, suggestion, s.guideNumbers(r.Context()))
		if len(missed) > 0 && (!body.AcknowledgeMisses || !dvr.SameMisses(body.AcknowledgedStarts, missed)) {
			apiError(w, http.StatusConflict, "missed_showings", dvr.MissedLine(missed), nil)
			return
		}
	}
	// Save the replacement before skipping. A failed save must leave the original airing in place.
	if fix.SetChannel != 0 && pass.ChannelID != fix.SetChannel {
		pass.ChannelID = fix.SetChannel
		if err := s.Store.UpdatePassRules(r.Context(), pass); err != nil {
			httpError(w, "pass not found", http.StatusNotFound)
			return
		}
	}
	if fix.OneShot != nil && !dvr.HaveOneShot(snap.passes, suggestion) {
		recs, _ := s.Store.Recordings(r.Context())
		shot := *fix.OneShot
		shot.LimitCount = dvr.OneShotLimit(shot, recs)
		if err := s.addOneShot(r.Context(), shot); err != nil {
			writeError(w, err)
			return
		}
		for _, other := range dvr.OneShotSkips(snap.airings, shot, suggestion) {
			if err := s.skipShowing(r.Context(), other); err != nil {
				writeError(w, err)
				return
			}
		}
	}
	if err := s.skipShowing(r.Context(), fix.Skip); err != nil {
		writeError(w, err)
		return
	}
	title := strings.TrimSpace(suggestion.Title)
	if title == "" {
		title = "The show"
	}
	_ = s.Store.AddEvent(r.Context(), "recording", fmt.Sprintf("%s will record on %s instead.", title, suggestion.Start.In(time.Local).Format("Jan 2 at 3:04 PM")))
	s.schedule(w, r)
}

func passIn(passes []store.Pass, id int64) (store.Pass, bool) {
	for _, pass := range passes {
		if pass.ID == id {
			return pass, true
		}
	}
	return store.Pass{}, false
}

func airingAt(airings []store.Airing, channelID int64, start time.Time) (store.Airing, bool) {
	for _, air := range airings {
		if air.ChannelID == channelID && air.Start.Equal(start) {
			return air, true
		}
	}
	return store.Airing{}, false
}

func (s *Server) skipShowing(ctx context.Context, air store.Airing) error {
	key, starts := dvr.SkipParts(air)
	if key == "" {
		return nil
	}
	return s.Store.SkipAiring(ctx, key, starts)
}

func (s *Server) addOneShot(ctx context.Context, shot store.Pass) error {
	before, err := s.Store.Passes(ctx)
	if err != nil {
		return err
	}
	if err := s.Store.AddPass(ctx, shot.Title, shot.ChannelID, shot.PadBefore, shot.PadAfter); err != nil {
		return err
	}
	after, err := s.Store.Passes(ctx)
	if err != nil {
		return err
	}
	id := newPassID(before, after)
	if id == 0 {
		return fmt.Errorf("the pass was not saved")
	}
	shot.ID = id
	return s.Store.UpdatePassRules(ctx, shot)
}

func newPassID(before, after []store.Pass) int64 {
	have := map[int64]struct{}{}
	for _, pass := range before {
		have[pass.ID] = struct{}{}
	}
	var id int64
	for _, pass := range after {
		if _, ok := have[pass.ID]; ok {
			continue
		}
		if pass.ID > id {
			id = pass.ID
		}
	}
	return id
}

func (s *Server) guideNumbers(ctx context.Context) map[int64]string {
	channels, err := s.Store.Channels(ctx, false)
	if err != nil {
		return nil
	}
	out := make(map[int64]string, len(channels))
	for _, ch := range channels {
		number := ch.DisplayNumber
		if number == "" {
			number = ch.GuideNumber
		}
		if number != "" {
			out[ch.ID] = number
		}
	}
	return out
}

// liveWarning is empty when this channel can take a tuner without leaving a
// recording short. A channel already on a tuned mux shares that tuner.
func (s *Server) liveWarning(ctx context.Context, channelID int64) string {
	ch, err := s.Store.SourceChannel(ctx, channelID)
	if err != nil {
		return ""
	}
	tuned, busy := s.tunersInUse(ctx, channelID)
	if s.onTunedMux(ctx, ch, tuned) {
		return ""
	}
	allow, warning := dvr.LiveWatch(s.tunerCount(ctx), busy, s.upcomingSoon(ctx, ch, tuned), s.now())
	if allow {
		return ""
	}
	return warning
}

func (s *Server) tunersInUse(ctx context.Context, channelID int64) (map[string]struct{}, int) {
	tuned := map[string]struct{}{}
	busy := 0
	if s.Hub != nil {
		// Status is also read when the tune starts. Don't make a quiet tuner add another long wait.
		statusCtx, cancel := context.WithTimeout(ctx, time.Second)
		list, err := s.Hub.Tuners(statusCtx)
		cancel()
		if err == nil {
			for _, tuner := range list {
				if tuner.Guide == "" && tuner.Target == "" && !tuner.Ours {
					continue
				}
				busy++
				if tuner.Guide != "" {
					tuned[tuner.Guide] = struct{}{}
				}
			}
		}
	}
	return tuned, busy + s.recordingBusy(ctx, channelID, tuned)
}

func (s *Server) recordingBusy(ctx context.Context, except int64, tuned map[string]struct{}) int {
	recs, err := s.Store.Recordings(ctx)
	if err != nil {
		return 0
	}
	seen := map[int64]struct{}{}
	n := 0
	for _, rec := range recs {
		if rec.Status != "recording" || rec.ChannelID == 0 || rec.ChannelID == except {
			continue
		}
		if rec.GuideNumber != "" {
			if _, ok := tuned[rec.GuideNumber]; ok {
				continue
			}
		}
		if _, ok := seen[rec.ChannelID]; ok {
			continue
		}
		seen[rec.ChannelID] = struct{}{}
		n++
	}
	return n
}

func (s *Server) onTunedMux(ctx context.Context, ch store.SourceChannel, tuned map[string]struct{}) bool {
	if ch.GuideNumber != "" {
		if _, ok := tuned[ch.GuideNumber]; ok {
			return true
		}
	}
	if ch.DisplayNumber != "" {
		if _, ok := tuned[ch.DisplayNumber]; ok {
			return true
		}
	}
	if ch.FrequencyHz <= 0 {
		return false
	}
	channels, err := s.Store.Channels(ctx, false)
	if err != nil {
		return false
	}
	for _, other := range channels {
		if other.ID == ch.ID {
			continue
		}
		if _, ok := tuned[other.GuideNumber]; !ok {
			continue
		}
		src, err := s.Store.SourceChannel(ctx, other.ID)
		if err != nil || src.FrequencyHz == 0 {
			continue
		}
		if src.FrequencyHz == ch.FrequencyHz {
			return true
		}
	}
	return false
}

func (s *Server) upcomingSoon(ctx context.Context, watch store.SourceChannel, tuned map[string]struct{}) []dvr.Soon {
	now := s.now()
	passes, err := s.Store.Passes(ctx)
	if err != nil || len(passes) == 0 {
		return nil
	}
	from := now.Add(-time.Minute)
	to := now.Add(31 * time.Minute)
	airings, err := s.Store.Airings(ctx, from, to)
	if err != nil {
		return nil
	}
	items := dvr.Plan(passes, airings, s.tunerCount(ctx), from, to)
	recs, _ := s.Store.Recordings(ctx)
	seen, _ := s.Store.SeenDeleted(ctx)
	skips, _ := s.Store.Skips(ctx)
	items = dvr.ApplyLibrary(items, passes, recs, seen, skips)
	var out []dvr.Soon
	for _, item := range items {
		if item.Skipped || s.sameMux(ctx, watch, item.Airing.ChannelID, tuned) {
			continue
		}
		out = append(out, dvr.Soon{
			Title:     item.Airing.Title,
			ChannelID: item.Airing.ChannelID,
			Start:     item.Airing.Start,
			Pad:       time.Duration(item.PadBefore) * time.Minute,
		})
	}
	return out
}

func (s *Server) sameMux(ctx context.Context, watch store.SourceChannel, channelID int64, tuned map[string]struct{}) bool {
	if channelID == watch.ID {
		return true
	}
	other, err := s.Store.SourceChannel(ctx, channelID)
	if err != nil {
		return false
	}
	if watch.FrequencyHz > 0 && other.FrequencyHz == watch.FrequencyHz {
		return true
	}
	if other.GuideNumber != "" {
		if _, ok := tuned[other.GuideNumber]; ok {
			return true
		}
	}
	return false
}

func (s *Server) tunerCount(ctx context.Context) int {
	devices, err := s.Store.Devices(ctx)
	if err != nil || len(devices) == 0 {
		return 2
	}
	n := 0
	for _, device := range devices {
		n += device.TunerCount
	}
	if n < 1 {
		return 2
	}
	return n
}

func (s *Server) passes(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.Passes(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if list == nil {
		list = []store.Pass{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"passes": list})
}

func (s *Server) addPass(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Title     string `json:"title"`
		ChannelID int64  `json:"channelId"`
		PadBefore *int   `json:"padBefore"`
		PadAfter  *int   `json:"padAfter"`
	}
	if err := decodeJSON(r, &body); err != nil || strings.TrimSpace(body.Title) == "" {
		httpError(w, "title required", http.StatusBadRequest)
		return
	}
	before, after := 1, 2
	if body.PadBefore != nil {
		before = clampPad(*body.PadBefore)
	}
	if body.PadAfter != nil {
		after = clampPad(*body.PadAfter)
	}
	if err := s.Store.AddPass(r.Context(), strings.TrimSpace(body.Title), body.ChannelID, before, after); err != nil {
		writeError(w, err)
		return
	}
	s.passes(w, r)
}

func (s *Server) updatePass(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, "invalid pass", http.StatusBadRequest)
		return
	}
	var body map[string]any
	if err := decodeJSON(r, &body); err != nil {
		httpError(w, "invalid json", http.StatusBadRequest)
		return
	}
	current, err := s.passByID(r.Context(), id)
	if err != nil {
		httpError(w, "pass not found", http.StatusNotFound)
		return
	}
	if v, ok := body["padBefore"]; ok {
		current.PadBefore = clampPad(int(num(v)))
	}
	if v, ok := body["padAfter"]; ok {
		current.PadAfter = clampPad(int(num(v)))
	}
	if v, ok := body["priority"]; ok {
		current.Priority = clampPriority(int(num(v)))
	}
	if v, ok := body["episodes"].(string); ok && v != "" {
		current.Episodes = v
	}
	if v, ok := body["keepMode"].(string); ok && v != "" {
		current.KeepMode = v
	}
	if v, ok := body["keepCount"]; ok {
		current.KeepCount = int(num(v))
	}
	if v, ok := body["limitCount"]; ok {
		current.LimitCount = int(num(v))
	}
	if v, ok := body["rerecord"].(bool); ok {
		current.Rerecord = v
	}
	if v, ok := body["commercials"].(bool); ok {
		current.Commercials = v
	}
	if v, ok := body["timeStart"].(string); ok {
		current.TimeStart = v
	}
	if v, ok := body["timeEnd"].(string); ok {
		current.TimeEnd = v
	}
	if v, ok := body["matchKind"].(string); ok && v != "" {
		current.MatchKind = v
	}
	if v, ok := body["channelId"]; ok {
		current.ChannelID = int64(num(v))
	}
	current.ID = id
	if err := s.Store.UpdatePassRules(r.Context(), current); err != nil {
		httpError(w, "pass not found", http.StatusNotFound)
		return
	}
	s.passes(w, r)
}

func (s *Server) passByID(ctx context.Context, id int64) (store.Pass, error) {
	list, err := s.Store.Passes(ctx)
	if err != nil {
		return store.Pass{}, err
	}
	for _, pass := range list {
		if pass.ID == id {
			return pass, nil
		}
	}
	return store.Pass{}, sql.ErrNoRows
}

func (s *Server) listingFor(ctx context.Context, channelID int64, title string) (store.Airing, bool) {
	now := time.Now()
	rows, err := s.Store.Airings(ctx, now.Add(-3*time.Hour), now.Add(8*time.Hour))
	if err != nil {
		return store.Airing{}, false
	}
	var current, next *store.Airing
	for i := range rows {
		row := &rows[i]
		if row.ChannelID != channelID {
			continue
		}
		if title != "" && strings.EqualFold(row.Title, title) && row.End.After(now) {
			return *row, true
		}
		if !now.Before(row.Start) && now.Before(row.End) && current == nil {
			current = row
		}
		if row.Start.After(now) && (next == nil || row.Start.Before(next.Start)) {
			next = row
		}
	}
	if current != nil {
		return *current, true
	}
	if next != nil && next.Start.Before(now.Add(2*time.Minute)) {
		return *next, true
	}
	return store.Airing{}, false
}

func num(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	default:
		return 0
	}
}

func clampPriority(priority int) int {
	if priority < 0 {
		return 0
	}
	if priority > 100 {
		return 100
	}
	return priority
}

func clampPad(minutes int) int {
	if minutes < 0 {
		return 0
	}
	if minutes > 30 {
		return 30
	}
	return minutes
}

func (s *Server) media(w http.ResponseWriter, r *http.Request) {
	if s.Hub == nil {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/media/live/"), "/")
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	channelID, err := strconv.ParseInt(parts[0], 10, 64)
	key, name := parts[1], parts[2]
	if _, ok := live.ParseRenditionKey(key); err != nil || !ok {
		http.NotFound(w, r)
		return
	}
	s.Hub.Touch(channelID, key)
	w.Header().Set("Cache-Control", "no-cache")
	if name == "index.m3u8" {
		if msn, part, ok := blockReload(r); ok {
			s.Hub.WaitMedia(channelID, key, msn, part, 1500*time.Millisecond)
		}
		body, err := s.Hub.Playlist(channelID, key)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Query().Get("_HLS_skip") {
		case "YES", "v2":
			body = live.DeltaPlaylist(body)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write(body)
		return
	}
	var contentType string
	switch {
	case strings.Contains(name, ".."):
	case name == "init.mp4":
		contentType = "video/mp4"
	case (strings.HasPrefix(name, "seg") || strings.HasPrefix(name, "part")) && strings.HasSuffix(name, ".m4s"):
		contentType = "video/iso.segment"
	case strings.HasPrefix(name, "seg") && strings.HasSuffix(name, ".ts"):
		contentType = "video/mp2t"
	}
	if contentType == "" {
		http.NotFound(w, r)
		return
	}
	path := filepath.Join(s.Hub.Dir, "live", parts[0], key, name)
	if _, err := os.Stat(path); err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", contentType)
	// A restarted rendition reuses seg00000 and part00000. An hour-long cache
	// would play the previous file under that name.
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeFile(w, r, path)
}

func decodeJSON(r *http.Request, dest any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(dest)
}

// blockReload reads an LL-HLS blocking playlist request. A missing part waits
// for the whole segment.
func blockReload(r *http.Request) (msn, part int, ok bool) {
	raw := r.URL.Query().Get("_HLS_msn")
	if raw == "" {
		return 0, 0, false
	}
	msn, err := strconv.Atoi(raw)
	if err != nil {
		return 0, 0, false
	}
	part = -1
	if p := r.URL.Query().Get("_HLS_part"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			part = n
		}
	}
	return msn, part, true
}

// waitServable returns once the playlist has a segment a player can fetch.
// A playlist that lists only parts is not enough: hls.js treats that as empty
// and waits out its retry. The first part still anchors the clock while this waits.
func waitServable(h *live.Hub, channelID int64, key string, d time.Duration) {
	if h == nil || key == "" {
		return
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		// A restart replaces the gate. Spending the whole deadline on the
		// old one hides the playlist the new encode is writing.
		slice := 100 * time.Millisecond
		if remain := time.Until(deadline); remain < slice {
			slice = remain
		}
		started := time.Now()
		// Segment 0. A negative part means the whole segment, not an open part.
		h.WaitMedia(channelID, key, 0, -1, slice)
		body, err := h.Playlist(channelID, key)
		if err == nil && strings.Count(string(body), "#EXTINF") >= 1 {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		// No gate yet, or this wake was for a playlist a restart already removed.
		if time.Since(started) < 50*time.Millisecond {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
