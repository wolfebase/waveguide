import type Hls from "hls.js";
import { events, type RoomState } from "./events";

export type SyncStatus = {
  state: "off" | "waiting" | "syncing" | "locked";
  /** Local minus target, in ms. Positive means this screen is ahead. */
  drift: number;
  members: number;
  room?: RoomState;
};

const TRIM_MS = 20;
const SEEK_MS = 400;
const MAX_TRIM = 0.03;
// Speeding up, or seeking forward, with less media than this underruns the live edge.
const CUSHION_S = 1.5;

type Frag = { start: number; duration: number; programDateTime: number | null };

/**
 * Holds a live <video> on a room's shared timeline. Every rendition stamps
 * segments with the same program date-time, so the target is the same frame on
 * every screen. Position is mapped through the playlist's segments in both
 * directions; small drift is corrected by rate, large drift by seeking.
 */
export class SyncEngine {
  private state: RoomState | null = null;
  private timer = 0;
  private unsubscribe: (() => void) | null = null;
  private status: SyncStatus = { state: "off", drift: 0, members: 0 };
  private lastSeek = 0;
  private holdUntil = 0;

  constructor(
    private video: HTMLVideoElement,
    private hls: Hls | null,
    private room: string,
    private channelId: number,
    private onStatus: (s: SyncStatus) => void,
  ) {}

  start() {
    const bus = events();
    this.unsubscribe = bus.on("sync.state", (data) => {
      const st = data as RoomState;
      if (st.room !== this.room) return;
      this.state = st;
      this.apply();
    });
    bus.join(this.room, this.channelId);
    const cached = bus.roomState(this.room) as RoomState | undefined;
    if (cached?.room === this.room) this.state = cached;
    this.setStatus({ state: "waiting", drift: 0, members: this.state?.members ?? 0 });
    this.timer = window.setInterval(() => this.apply(), 250);
  }

  stop() {
    window.clearInterval(this.timer);
    this.unsubscribe?.();
    events().leave(this.room);
    this.video.playbackRate = 1;
    this.setStatus({ state: "off", drift: 0, members: 0 });
  }

  command(action: "play" | "pause" | "seek" | "live", mediaTime?: number) {
    events().command(this.room, action, mediaTime ? { mediaTime } : {});
  }

  private frags(): Frag[] {
    const h = this.hls as unknown as { levels?: { details?: { fragments?: Frag[] } }[]; currentLevel?: number; loadLevel?: number } | null;
    if (!h?.levels?.length) return [];
    const level = h.levels[Math.max(0, h.currentLevel ?? h.loadLevel ?? 0)] ?? h.levels[0];
    return level?.details?.fragments ?? [];
  }

  /** Program date-time (Unix ms) of the frame on screen. */
  mediaNow(): number | null {
    const t = this.video.currentTime;
    const frags = this.frags();
    if (frags.length) {
      for (const f of frags) {
        if (f.programDateTime == null || f.duration <= 0 || f.duration > 30) continue;
        if (t >= f.start && t < f.start + f.duration) return f.programDateTime + (t - f.start) * 1000;
      }
      return null;
    }
    const native = (this.video as HTMLVideoElement & { getStartDate?: () => Date }).getStartDate?.();
    if (native && !Number.isNaN(native.getTime())) return native.getTime() + t * 1000;
    return null;
  }

  /** Playhead position for a program date-time, or null when the playlist does not hold it yet. */
  private timeFor(media: number): number | null {
    const frags = this.frags().filter((f) => f.programDateTime != null && f.duration > 0 && f.duration <= 30);
    if (frags.length) {
      for (const f of frags) {
        const s = f.programDateTime as number;
        if (media >= s && media < s + f.duration * 1000) return f.start + (media - s) / 1000;
      }
      return null;
    }
    const native = (this.video as HTMLVideoElement & { getStartDate?: () => Date }).getStartDate?.();
    if (native && !Number.isNaN(native.getTime())) return (media - native.getTime()) / 1000;
    return null;
  }

  /** Seconds of media buffered past the playhead. Zero when the playhead is already past it. */
  private forwardMedia(): number {
    const t = this.video.currentTime;
    const ranges = this.video.buffered;
    let ahead = 0;
    for (let i = 0; i < ranges.length; i++) {
      const end = ranges.end(i);
      if (t >= ranges.start(i) - 0.05 && t <= end) ahead = Math.max(ahead, end - t);
    }
    return ahead;
  }

  /**
   * Moves this screen onto the target. Ahead: pause for exactly the drift while the
   * buffer keeps filling (backward seeks in a live buffer are fragile). Behind: seek forward.
   */
  private correct(drift: number, target: number) {
    if (drift > 0) {
      const now = performance.now();
      if (now < this.holdUntil) return;
      this.holdUntil = now + drift;
      this.video.pause();
      window.setTimeout(() => {
        this.holdUntil = 0;
        void this.video.play().catch(() => undefined);
      }, drift);
      return;
    }
    this.seekTo(target);
  }

  /** Seeks sparingly: every seek flushes the player, and seeking into data it does not have stalls it. */
  private seekTo(media: number): boolean {
    const now = performance.now();
    if (now - this.lastSeek < 2000) return false;
    const t = this.timeFor(media);
    if (t == null) return false;
    this.lastSeek = now;
    this.video.currentTime = t;
    return true;
  }

  private setStatus(s: SyncStatus) {
    this.status = s;
    this.onStatus(s);
  }

  private apply() {
    const st = this.state;
    if (!st) return;
    const video = this.video;
    if (performance.now() < this.holdUntil) return;
    const local = this.mediaNow();
    if (local == null || video.readyState < 2 || video.seeking) {
      this.setStatus({ ...this.status, state: "waiting", members: st.members, room: st });
      return;
    }
    const target = st.rate === 0 ? st.anchorMedia : st.anchorMedia + (events().serverNow() - st.anchorServer) * st.rate;
    const drift = local - target;
    video.dataset.syncOffset = String(Math.round(local - Date.now()));
    video.dataset.syncDrift = String(Math.round(drift));
    if (st.rate === 0) {
      if (!video.paused) video.pause();
      if (Math.abs(drift) > TRIM_MS * 2) this.seekTo(target);
      this.setStatus({ state: "locked", drift, members: st.members, room: st });
      return;
    }
    if (video.paused) void video.play().catch(() => undefined);
    const ahead = this.forwardMedia();
    if (Math.abs(drift) > SEEK_MS) {
      video.playbackRate = 1;
      // The target is not buffered, or the cushion is too thin to chase it.
      // Hold rate at 1; a seek past the edge stalls the picture.
      if (drift < 0 && (this.timeFor(target) == null || ahead < CUSHION_S)) {
        this.setStatus({ state: "syncing", drift, members: st.members, room: st });
        return;
      }
      this.correct(drift, target);
      this.setStatus({ state: "syncing", drift, members: st.members, room: st });
      return;
    }
    if (Math.abs(drift) > TRIM_MS) {
      let rate = 1 + Math.max(-MAX_TRIM, Math.min(MAX_TRIM, -drift / 2000));
      if (rate > 1 && ahead < CUSHION_S) rate = 1;
      video.playbackRate = rate;
      this.setStatus({ state: "syncing", drift, members: st.members, room: st });
      return;
    }
    video.playbackRate = 1;
    this.setStatus({ state: "locked", drift, members: st.members, room: st });
  }
}
