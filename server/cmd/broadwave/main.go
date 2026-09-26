package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"broadwave/internal/backup"
	"broadwave/internal/discovery"
	"broadwave/internal/doctor"
	"broadwave/internal/dvr"
	"broadwave/internal/guide"
	"broadwave/internal/httpapi"
	"broadwave/internal/live"
	"broadwave/internal/logbuf"
	"broadwave/internal/psip"
	"broadwave/internal/realtime"
	"broadwave/internal/source"
	"broadwave/internal/sports"
	"broadwave/internal/store"
	"broadwave/internal/update"
)

//go:embed all:assets
var embedded embed.FS

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	addr := flag.String("addr", ":8477", "listen address")
	configDir := flag.String("config", "data", "directory for the catalog database")
	dev := flag.Bool("dev", false, "allow a local Vite dev server to call the API")
	hdhrHost := flag.String("hdhr", os.Getenv("HDHR_HOST"), "tuner address when the container cannot hear broadcast discovery")
	bonjour := flag.Bool("bonjour", true, "advertise this server to the apps over Bonjour")
	healthcheck := flag.Bool("healthcheck", false, "check a running server on -addr and exit (for container health checks)")
	staging := flag.Bool("staging", false, "test copy beside a real server: never record, scan, or pull the guide; tune only when someone watches")
	flag.Parse()
	if *healthcheck {
		os.Exit(checkHealth(*addr))
	}
	logbuf.Install(os.Stderr)
	doctor.ApplyIdentity(filepath.Join(*configDir, "work", "recordings"))
	// Copy the catalog before Open migrates it, when this build is a new version.
	if err := backup.SnapshotIfVersionChanged(context.Background(), *configDir, version, time.Now()); err != nil {
		slog.Error(fmt.Sprintf("backup: %v", err))
	}

	st, err := store.Open(*configDir)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	defer st.Close()

	assets, err := fs.Sub(embedded, "assets/web")
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	work := filepath.Join(*configDir, "work")
	ffmpegPath, _ := execLook("ffmpeg")
	encoder := live.DetectEncoder(ffmpegPath)
	// BROADWAVE_BENCH=0 skips the startup encode. The relay check opens several
	// renditions at once, and a measured tile budget would refuse the extra ones.
	var host live.Host
	if os.Getenv("BROADWAVE_BENCH") != "0" {
		measureCtx, measureCancel := context.WithTimeout(context.Background(), 20*time.Second)
		host = live.MeasureHost(measureCtx, ffmpegPath, encoder)
		measureCancel()
		slog.Info("encoder: " + host.Line())
	}
	live.Reap(work)
	hub := live.New(st, work, ffmpegPath, encoder)
	hub.Host = host
	if *staging {
		slog.Info("staging: recordings, guide pulls, background tunes, and the tuner emulator are off")
	} else if err := dvr.Recover(context.Background(), st, time.Now(), func(rec store.Recording, left time.Duration) error {
		minutes := int(left / time.Minute)
		if minutes < 1 {
			return nil
		}
		_, err := hub.RecordMeta(context.Background(), minutes, store.Recording{
			ChannelID: rec.ChannelID, Title: rec.Title, Subtitle: rec.Subtitle,
			Description: rec.Description, Category: rec.Category, ProgramID: rec.ProgramID, GameID: rec.GameID,
		})
		if err != nil {
			slog.Error(fmt.Sprintf("recording: resume %s: %v", rec.Title, err))
		}
		return nil
	}); err != nil {
		slog.Error(fmt.Sprintf("recording: %v", err))
	}
	hub.OnSaved = func(rec store.Recording) {
		dvr.OnSaved(context.Background(), st, hub, rec)
	}
	slog.Info(fmt.Sprintf("encoder: %s deint: %s smooth: %s", encoder, hub.DeintBroadcast, hub.DeintSmooth))
	bus := realtime.NewBus()
	bus.MediaStart = hub.EarliestMedia
	st.OnEvent = func(ev store.Event) { bus.Publish("activity", ev) }
	hub.OnChange = debounce(500*time.Millisecond, func() { bus.Publish("live.changed", nil) })
	hub.OnMedia = func(channelID int64) {
		if earliest, ok := hub.EarliestMedia(channelID); ok {
			bus.Settle(channelID, earliest)
		}
	}
	api := &httpapi.Server{Store: st, Assets: assets, Dev: *dev, Hub: hub, Version: version, Bus: bus, Sports: sports.NewCache(sports.NewESPN()), Staging: *staging, BackupDir: filepath.Join(*configDir, "backups")}
	api.Updates = releaseCheck(st, version)
	hub.OnPSIP = func(_ int, g psip.Guide) {
		n, err := api.ApplyBroadcast(context.Background(), g)
		if err != nil {
			slog.Error(fmt.Sprintf("guide: broadcast: %v", err))
			return
		}
		if n > 0 {
			slog.Info(fmt.Sprintf("guide: broadcast filled %d listings", n))
		}
	}
	handler := api.Handler()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := source.Auto(ctx, st, nil, *hdhrHost)
		if err != nil {
			slog.Error(fmt.Sprintf("discovery: %v", err))
			return
		}
		slog.Info(fmt.Sprintf("discovery: %d device(s)", n))
		bus.Publish("sources.found", map[string]int{"found": n})
		// SiliconDust asks for a random 20-28 h gap after each successful pull.
		// A restart waits out whatever nextGuidePull was already stored.
		for !*staging {
			if wait := api.GuideDelay(time.Now()); wait > 0 {
				time.Sleep(wait)
				continue
			}
			refreshGuide(api)
		}
	}()
	go func() {
		tick := time.NewTicker(time.Hour)
		defer tick.Stop()
		for range tick.C {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			n, err := api.RefreshSources(ctx, time.Now())
			cancel()
			if err != nil {
				slog.Error(fmt.Sprintf("playlist refresh: %v", err))
				continue
			}
			if n > 0 {
				slog.Info(fmt.Sprintf("playlist refresh: %d", n))
			}
		}
	}()
	go func() {
		tick := time.NewTicker(5 * time.Minute)
		defer tick.Stop()
		for range tick.C {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			n, err := source.Auto(ctx, st, nil, *hdhrHost)
			cancel()
			if err != nil {
				slog.Error(fmt.Sprintf("discovery: %v", err))
				continue
			}
			bus.Publish("sources.found", map[string]int{"found": n})
		}
	}()
	go api.WatchHome(context.Background())
	go func() {
		time.Sleep(20 * time.Second)
		tick := time.NewTicker(10 * time.Minute)
		defer tick.Stop()
		api.LinkGames(context.Background())
		api.NoteTeams(context.Background())
		for range tick.C {
			api.LinkGames(context.Background())
			api.NoteTeams(context.Background())
		}
	}()
	go func() {
		time.Sleep(20 * time.Second)
		if !*staging {
			api.BroadcastScan(context.Background())
		}
	}()
	go func() {
		tick := time.NewTicker(20 * time.Second)
		defer tick.Stop()
		for range tick.C {
			if !*staging {
				dvr.Tick(context.Background(), st, hub)
				api.ExtendRecordings(context.Background())
			}
			if hub != nil {
				hub.ReleaseAbandoned(45 * time.Second)
				if !*staging {
					httpapi.SyncEmulator(st, hub)
				}
			}
		}
	}()

	discKey, keyErr := discovery.LoadKey(filepath.Join(*configDir, "discovery.key"))
	if keyErr != nil {
		slog.Error(fmt.Sprintf("discovery key: %v", keyErr))
		discKey = nil
	} else {
		api.DiscoveryKey = discovery.PublicKeyString(discKey)
	}
	if id, err := st.Identity(context.Background(), httpapi.DefaultServerName()); err != nil {
		slog.Error(fmt.Sprintf("identity: %v", err))
	} else {
		if finder, err := discovery.ListenFinder(portOf(*addr), id.ID, id.Name, discKey); err != nil {
			slog.Error(fmt.Sprintf("discovery: %v", err))
		} else {
			defer finder.Close()
		}
		if *bonjour {
			if advert, err := discovery.Announce(discovery.Advert{ID: id.ID, Name: id.Name, Version: version, Port: portOf(*addr)}); err != nil {
				slog.Error(fmt.Sprintf("bonjour: %v", err))
			} else {
				defer advert.Shutdown()
			}
		}
	}
	slog.Info(fmt.Sprintf("Broadwave listening on %s", *addr))
	server := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if api.Updates != nil {
		go api.Updates.Run(ctx)
	}
	go (&backup.Scheduler{Store: st, Dir: api.BackupDir}).Run(ctx)
	go func() {
		<-ctx.Done()
		hub.Shutdown()
		shut, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		_ = server.Shutdown(shut)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func releaseCheck(st *store.Store, version string) *update.Checker {
	c := update.New(version)
	c.Enabled = func(ctx context.Context) bool {
		on, err := st.UpdatesEnabled(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("update: %v", err))
			return false
		}
		return on
	}
	c.Load = func(ctx context.Context) (*update.Notice, time.Time) {
		ver, notes, message, at, err := st.SavedUpdate(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("update: %v", err))
			return nil, time.Time{}
		}
		if ver == "" || message == "" {
			return nil, at
		}
		return &update.Notice{Version: ver, NotesURL: notes, Message: message}, at
	}
	c.Save = func(ctx context.Context, n *update.Notice, at time.Time) {
		ver, notes, message := "", "", ""
		if n != nil {
			ver, notes, message = n.Version, n.NotesURL, n.Message
		}
		if err := st.SaveUpdate(ctx, ver, notes, message, at); err != nil {
			slog.Error(fmt.Sprintf("update: %v", err))
		}
	}
	return c
}

func refreshGuide(api *httpapi.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := api.RefreshGuide(ctx); err != nil {
		slog.Error(fmt.Sprintf("guide: %v", err))
		api.DeferGuide(ctx, guide.RetryAfterError)
	}
}

func checkHealth(addr string) int {
	client := http.Client{Timeout: 4 * time.Second}
	res, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/health", portOf(addr)))
	if err != nil {
		return 1
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func portOf(addr string) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 8477
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return 8477
	}
	return n
}

// debounce collapses bursts of calls into one call after d of quiet.
func debounce(d time.Duration, fn func()) func() {
	var mu sync.Mutex
	var t *time.Timer
	return func() {
		mu.Lock()
		defer mu.Unlock()
		if t != nil {
			t.Stop()
		}
		t = time.AfterFunc(d, fn)
	}
}

func execLook(name string) (string, error) {
	return exec.LookPath(name)
}
