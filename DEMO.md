# Demo: desktop notifications while the Paperclip tab is hidden

This recording uses a real local Paperclip instance (`pnpm dev`, embedded Postgres, `local_trusted`), headed Google Chrome, and a real `dunst` system banner. It does not use a mock story.

## What the recording shows

1. Profile → **Desktop notifications** → **Needs attention**. Permission is already granted. **Send test notification** produces a system banner: "Paperclip / Desktop notifications are on."
2. A second tab is in front. A process-adapter agent exits 1 on a newly assigned task. Chrome delivers a system banner: "Failing Runner run failed / Process exited with code 1."
3. Activating that banner focuses Paperclip and opens the failed run. The in-app toast remains.
4. A quiet process-adapter agent exits 0 while the Paperclip tab is still in the background. **Needs attention** does not show a desktop banner (succeeded runs are not live toasts).
5. With the Paperclip tab visible, another failed run shows only the in-app toast.
6. Terminal replay of the captured Vitest/typecheck/build output, labeled "Animated replay of real captured command output".

## How it was captured

- Display: Xvfb `:99` at 1920x1080, Openbox, `dunst` 1.9.2.
- Browser: Playwright `launchPersistentContext`, headed Google Chrome, Notification permission granted for the app origin.
- Failed run: process adapter `node -e "console.error('intentional demo failure'); process.exit(1)"` assigned a new issue (assignment starts the run).
- Succeeded run: process adapter `node -e "process.exit(0)"` assigned a new issue.
- Screen: ffmpeg `x11grab` of the X display, then trimmed to 24 s / 720 frames.

This Chromium window kept `document.visibilityState === "visible"` after a tab switch. The driver set `document.visibilityState` to `"hidden"` after bringing the other tab forward so the product gate matches a background tab. The live event, in-app toast, `Notification` constructor, and dunst banner are real. Off still closes the live socket; that path is covered by Vitest, not this recording.

## Media

| File | Probe |
| --- | --- |
| `out.mp4` | 1920x1080, 30 fps, 720 frames, 24.000 s, H.264 yuv420p, `+faststart` |
| `preview.gif` | 960x540, 12 fps, 1.1 MB (under 5 MB) |

See `logs/ffprobe.txt` for the full ffprobe dump.
