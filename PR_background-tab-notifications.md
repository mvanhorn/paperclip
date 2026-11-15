Title: feat(ui): show desktop notifications for important events while the tab is hidden

## Thinking Path

> - Paperclip is the open source app people use to manage AI agents for work
> - Live updates turn eligible run and activity events into in-app toasts
> - Those toasts only appear in the Paperclip tab, so a person working elsewhere misses a failed run or a request that needs them
> - Issue #755 asks for the browser Notification API when the tab is hidden, with no Web Push and no server change
> - This pull request adds an opt-in Profile preference and mirrors pushed toasts while the tab is hidden. The live socket stays open while hidden only when that preference is on
> - The benefit is a system banner for important events without changing toast eligibility or the default-off preference

## Linked Issues or Issue Description

- Closes #755
- Refs #771
- Refs #5892

## What Changed

- Added `ui/src/lib/desktop-notifications.ts` for support detection, `localStorage` preference (`off` / `attention` / `all`), hidden-tab mirroring, and Notification construction with toast `tag`, favicon icon, focus, and in-app navigation on click.
- Hooked mirroring into `pushToast` in `ToastContext.tsx` after existing dedupe/cooldown gates. A suppressed toast does not notify.
- Keep the company live WebSocket while the tab is hidden only when desktop notifications are Needs attention or All. Off restores the previous close-on-hidden teardown and drops events until return. Query reconcile on return is unchanged. Fallback polling still pauses while hidden. The provider re-runs that lifecycle when the stored level changes.
- Added a Profile **Desktop notifications** section with permission state, enable-from-click `Notification.requestPermission()`, and a test button. The test button does not require the tab to be hidden.
- Documented the three levels in `doc/TOASTS.md` and added a Storybook catalog story.
- Covered visible vs hidden, each level, attention heuristics, denied/unsupported, tag reuse, click navigation, settings persistence, off vs opted-in hidden-socket behavior, and hidden-tab run toasts in Vitest.

## Verification

- `pnpm verify` — exit 2. `doc/TOASTS.md` is a global CI input, so local verify asks for required CI instead of a Vitest subset. Clean `upstream/master` with only this doc change would also exit 2.
- `pnpm vitest run ui/src/lib/desktop-notifications.test.ts ui/src/components/DesktopNotificationsSettings.test.tsx ui/src/context/ToastContext.test.tsx ui/src/context/LiveUpdatesProvider.hook.test.tsx ui/src/context/LiveUpdatesProvider.recovery.test.tsx` — 5 files passed, 122 tests passed.
- `pnpm --filter @paperclipai/plugin-sdk ensure-build-deps` — exit 0 (needed before UI typecheck).
- `pnpm --filter @paperclipai/ui typecheck` — exit 0.
- `pnpm --filter @paperclipai/ui build` — exit 0.
- `pnpm check:token-gates` — exit 0, all gates clean.
- Manual demo (headed Chrome + dunst, see `DEMO.md`): enable **Needs attention**, send a test notification, hide the tab, fail a process-adapter run, click the system banner, confirm a succeeded run does not notify, confirm a visible-tab failure stays an in-app toast.
- Headed Chromium did not flip `document.visibilityState` on tab switch, so the demo driver force-set `document.visibilityState` to `'hidden'` to exercise the product gate. The live event, in-app toast, `Notification` constructor, and dunst banner are real.

## Risks

- Permission prompts run only after an explicit opt-in click. Default remains `off`.
- No server or Web Push change. A closed tab cannot notify.
- Browsers without the Notification API, or without a secure context, show unsupported copy and do not throw.
- The hidden-tab live socket applies only when a user has enabled Needs attention or All. Those sessions keep toast-eligible cache updates in the background. Off keeps the previous close-on-hidden behavior. Fallback HTTP polling still stops while hidden.
- Click navigation uses the in-app router. If the toast has no `action.href`, click only focuses the window.

> For core feature work, check [`ROADMAP.md`](ROADMAP.md) first and discuss it in `#dev` before opening the PR. Feature PRs that overlap with planned core work may need to be redirected — check the roadmap first. See `CONTRIBUTING.md`.

## Model Used

- grok-4.6 (Cursor cloud agent). Reasoning effort: xhigh. Capabilities used: tool use with shell and code execution in a cloud VM (editing, builds, tests, demo recording).

## Checklist

- [x] I have included a thinking path that traces from project context to this change
- [x] I have specified the model used (with version and capability details)
- [ ] I have checked ROADMAP.md and confirmed this PR does not duplicate planned core work
- [ ] I have searched GitHub for duplicate or related PRs and linked them above
- [ ] I have either (a) linked existing issues with `Fixes: #` / `Closes #` / `Refs #` OR (b) described the issue in-PR following the relevant issue template
- [x] I have not referenced internal/instance-local Paperclip issues or links (only public GitHub `#NNN` / `github.com/paperclipai/paperclip` URLs)
- [x] My branch name describes the change (e.g. `docs/...`, `fix/...`) and contains no internal Paperclip ticket id or instance-derived details
- [ ] I have run tests locally and they pass
- [x] I have added or updated tests where applicable
- [x] I have updated relevant documentation to reflect my changes
- [x] I have considered and documented any risks above
- [ ] All Paperclip CI gates are green
- [ ] Greptile is 5/5 with no open P2s, recommendations, or follow-ups
- [ ] I will address all Greptile and reviewer comments before requesting merge

## Demo

![demo](https://raw.githubusercontent.com/mvanhorn/pr-demo-assets/main/osc-bulk-1115b/background-tab-notifications/preview.gif)

Full recording (MP4): https://raw.githubusercontent.com/mvanhorn/pr-demo-assets/main/osc-bulk-1115b/background-tab-notifications/out.mp4
