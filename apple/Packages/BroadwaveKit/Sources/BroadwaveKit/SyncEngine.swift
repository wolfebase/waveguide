import AVFoundation
import Foundation
import Observation

/// Holds an AVPlayer on a Whole-Home Sync room's timeline.
///
/// Segments carry EXT-X-PROGRAM-DATE-TIME on the channel's shared timeline, so
/// `currentDate()` is the same wall time for the same frame on every screen.
/// Small drift is trimmed with rate, a screen that is ahead pauses for exactly
/// its drift, and a screen that is behind seeks forward by date.
@MainActor
@Observable
public final class SyncEngine {
    public enum State: String, Sendable { case off, waiting, syncing, locked }

    public private(set) var state: State = .off
    public private(set) var drift: Double = 0
    public private(set) var members = 0

    /// When set, a frame counts only after the layer is showing it.
    /// Presentation size arrives first, and pausing then freezes the tile.
    public var displayedFrame: (@MainActor () -> Bool)?

    private let player: AVPlayer
    private let socket: EventSocket
    private let room: String
    private let channelID: Int64
    private var room_: RoomState?
    private var handler: UUID?
    private var timer: Timer?
    private var holdUntil = Date.distantPast
    private var lastSeek = Date.distantPast

    static let trimMS = 20.0
    static let seekMS = 400.0
    static let maxTrim = 0.03

    /// What one sync tick should do. A pause before the first decoded frame
    /// leaves the layer black, and a seek into a date the playlist does not
    /// hold stalls the same way.
    enum SyncMove: Equatable {
        case wait
        case pause(resumeAfter: Double?, seekToTarget: Bool)
        case seek
        case rate(Float, locked: Bool)
    }

    static func decide(hasFrame: Bool, driftMS: Double, roomRate: Double, canSeek: Bool, forwardBuffer: Double = 2) -> SyncMove {
        guard hasFrame else { return .wait }
        if roomRate == 0 {
            return .pause(resumeAfter: nil, seekToTarget: canSeek && abs(driftMS) > trimMS * 2)
        }
        if abs(driftMS) > seekMS {
            if driftMS > 0 {
                return .pause(resumeAfter: driftMS / 1000, seekToTarget: false)
            }
            // Chasing a target the buffer does not hold lands past the live edge.
            if !canSeek {
                return .wait
            }
            if forwardBuffer < 1.5 {
                return .rate(1, locked: false)
            }
            return .seek
        }
        if abs(driftMS) > trimMS {
            var trimmed = Float(1 + max(-maxTrim, min(maxTrim, -driftMS / 2000)))
            if trimmed > 1, forwardBuffer < 1.5 {
                trimmed = 1
            }
            return .rate(trimmed, locked: false)
        }
        return .rate(1, locked: true)
    }

    /// The room is moving and this player is not. AVPlayer can report
    /// `.playing` with rate 0, and that state never paints the next frame.
    static func shouldKeepPlaying(roomRate: Double, paused: Bool, rate: Float) -> Bool {
        roomRate != 0 && (paused || rate == 0)
    }

    public init(player: AVPlayer, socket: EventSocket, room: String, channelID: Int64) {
        self.player = player
        self.socket = socket
        self.room = room
        self.channelID = channelID
    }

    public func start() {
        handler = socket.on("sync.state") { [weak self] data in
            guard let self, let st = try? JSONDecoder().decode(RoomState.self, from: data), st.room == self.room else { return }
            room_ = st
            members = st.members
            apply()
        }
        socket.join(room: room, channelID: channelID)
        state = .waiting
        if let cached = socket.roomState(room), let st = try? JSONDecoder().decode(RoomState.self, from: cached), st.room == room {
            room_ = st
            members = st.members
            apply()
        }
        timer = Timer.scheduledTimer(withTimeInterval: 0.25, repeats: true) { [weak self] _ in
            Task { @MainActor in self?.apply() }
        }
    }

    public func stop() {
        timer?.invalidate()
        if let handler {
            socket.off("sync.state", handler)
        }
        socket.leave(room: room)
        player.rate = player.rate == 0 ? 0 : 1
        state = .off
    }

    public func command(_ action: String, mediaTime: Double? = nil) {
        socket.command(room: room, action: action, mediaTime: mediaTime)
    }

    /// Program date-time of the frame on screen, Unix ms.
    public func mediaNow() -> Double? {
        guard let date = player.currentItem?.currentDate() else { return nil }
        return date.timeIntervalSince1970 * 1000
    }

    private func apply() {
        guard let st = room_, Date() >= holdUntil else { return }
        guard let item = player.currentItem, item.status == .readyToPlay, let local = mediaNow() else {
            state = .waiting
            return
        }
        let target = st.rate == 0 ? st.anchorMedia : st.target(atServer: socket.serverNow())
        let d = local - target
        drift = d
        let sized = item.presentationSize.width > 0 && item.presentationSize.height > 0
        let hasFrame = displayedFrame?() ?? sized
        // Pausing again on the tick that restarts a stuck player puts rate
        // straight back to 0, and the tile stays on one frame.
        if Self.shouldKeepPlaying(roomRate: st.rate, paused: player.timeControlStatus == .paused, rate: player.rate) {
            player.play()
            state = hasFrame ? .syncing : .waiting
            return
        }
        switch Self.decide(hasFrame: hasFrame, driftMS: d, roomRate: st.rate, canSeek: canSeek(to: target, item: item), forwardBuffer: bufferedAhead(item)) {
        case .wait:
            state = .waiting
        case let .pause(resumeAfter, seekToTarget):
            player.pause()
            if seekToTarget {
                seek(to: target)
            }
            if let resumeAfter {
                holdUntil = Date().addingTimeInterval(resumeAfter)
                DispatchQueue.main.asyncAfter(deadline: .now() + resumeAfter) { [weak self] in
                    self?.player.play()
                }
                state = .syncing
            } else {
                state = .locked
            }
        case .seek:
            seek(to: target)
            state = .syncing
        case let .rate(rate, locked):
            if player.rate != rate {
                player.rate = rate
            }
            state = locked ? .locked : .syncing
        }
    }

    /// Seconds of media loaded past the playhead.
    private func bufferedAhead(_ item: AVPlayerItem) -> Double {
        let now = CMTimeGetSeconds(item.currentTime())
        if !now.isFinite {
            return 0
        }
        var ahead = 0.0
        for value in item.loadedTimeRanges {
            let range = value.timeRangeValue
            let start = CMTimeGetSeconds(range.start)
            let dur = CMTimeGetSeconds(range.duration)
            if !start.isFinite || !dur.isFinite {
                continue
            }
            let end = start + dur
            if now >= start - 0.05, now <= end {
                ahead = max(ahead, end - now)
            }
        }
        return ahead
    }

    /// The target date has to fall in a range the item can already play.
    /// Seeking earlier than the first segment, or past the live edge, stalls.
    private func canSeek(to mediaMS: Double, item: AVPlayerItem) -> Bool {
        guard let current = item.currentDate() else { return false }
        let delta = mediaMS / 1000 - current.timeIntervalSince1970
        let target = CMTimeAdd(item.currentTime(), CMTime(seconds: delta, preferredTimescale: 90000))
        return item.seekableTimeRanges.contains { CMTimeRangeContainsTime($0.timeRangeValue, time: target) }
    }

    private func seek(to media: Double) {
        guard Date().timeIntervalSince(lastSeek) > 2, let item = player.currentItem else { return }
        guard canSeek(to: media, item: item) else { return }
        lastSeek = Date()
        item.seek(to: Date(timeIntervalSince1970: media / 1000)) { _ in }
    }
}
