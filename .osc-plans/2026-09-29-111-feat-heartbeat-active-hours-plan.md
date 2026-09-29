---
generated_by: osc-newfeature
bulk_run: true
type: feat
repo: paperclipai/paperclip
plan_id: 111
date: 2026-09-29
slug: heartbeat-active-hours
title: "feat: timer heartbeat active hours"
feature_complexity: M
deep_discovery_report: true
deep_rubric_score: 9
dogfooded_general: false
video_required: false
PROCESS_LEVEL: PR_WELCOME
---

# feat: timer heartbeat active hours

## Problem

Timer heartbeats fire whenever `runtimeConfig.heartbeat.enabled` is true and `intervalSec` has elapsed. Daily run and spend caps limit volume, not wall-clock time. Operators who want agents to run only during a business window must disable the timer entirely, or they pay for overnight wakes that do no useful work. Event-driven wakes (comments, assignments, approvals, monitors, on-demand ping) must keep working outside that window.

## Behavior

Add an optional per-agent window on the existing heartbeat policy:

```json
{
  "heartbeat": {
    "enabled": true,
    "intervalSec": 300,
    "activeHours": {
      "start": "09:00",
      "end": "18:00",
      "timezone": "America/New_York"
    }
  }
}
```

Rules:

1. Omit `activeHours` (or set it null) and timer scheduling stays as it is today.
2. When the window is set, `heartbeatService.tickTimers(now)` skips enqueue for that agent if `now` is outside the window. Count those agents in the existing `skipped` tally. Do not claim the timer slot and do not write a heartbeat run.
3. Overnight windows wrap midnight: `start > end` means `[start, 24:00)` ∪ `[00:00, end)`. Same rule already used by status-card active hours.
4. Event-driven wakes are unchanged: comment, assignment, approval, monitor, recovery, and on-demand sources still enqueue.
5. Paused, terminated, invokable-false, and hard-budget-stopped agents still do not receive timer wakes.
6. Agent create/update rejects an invalid IANA timezone or a `start`/`end` that is not `HH:MM` 24-hour. Do not silently drop a bad window.
7. Board UI: on the agent Run Policy section, when heartbeat-on-interval is enabled, show an optional "Limit timer heartbeats to active hours" control with start, end, and timezone. Hide the three fields when the toggle is off. Persist through the existing `runtimeConfig.heartbeat` patch path.
8. New-agent create uses the same optional fields when the create Run Policy section is shown.
9. Do not add first-party telemetry events. Do not append run-log rows for a skipped timer tick.

## Files

Implementation:

- `packages/shared/src/active-hours.ts` — new. Export `ActiveHoursWindow`, `isValidTimeZone`, `parseHmToMinutes`, `isWithinActiveHours(window, now)`, `activeHoursWindowSchema` (Zod: `HH:MM` + IANA timezone). Overnight wrap lives here.
- `packages/shared/src/index.ts` — export the helper and schema.
- `packages/shared/src/validators/status-card.ts` — reuse `activeHoursWindowSchema` / `isValidTimeZone` instead of the local copy.
- `packages/shared/src/validators/agent.ts` — extend `agentRuntimeConfigSchema` with an optional `heartbeat` object that includes `enabled`, `intervalSec`, and `activeHours` (nullable/optional). Keep the existing catchall so unknown heartbeat keys still persist.
- `packages/shared/src/types/heartbeat.ts` — add `HeartbeatActiveHours` and attach it to the documented policy shape if a policy type already lives here; otherwise add `HeartbeatTimerPolicy` next to the run types.
- `server/src/services/heartbeat.ts` — `parseHeartbeatPolicy` reads `activeHours` (null when absent or incomplete). `tickTimers` calls `isWithinActiveHours` after the enabled/interval checks and before `claimDueTimerHeartbeat`. Do not change `enqueueWakeup` for non-timer sources.
- `server/src/services/status-card-update-engine.ts` — `isWithinStatusCardActiveHours` delegates to the shared helper so both surfaces stay identical.
- `ui/src/components/agent-config-primitives.tsx` — help text for the active-hours control.
- `ui/src/components/agent-config-defaults.ts` — no default window (omit).
- `ui/src/lib/new-agent-runtime-config.ts` — pass through optional `activeHours` when the create form sets it; default remains omitted.
- `ui/src/lib/new-agent-hire-payload.ts` — include `activeHours` in the hire runtimeConfig when present.
- `ui/src/lib/agent-config-patch.ts` — treat `heartbeat.activeHours` as a patchable heartbeat field (set object or clear to null).
- `ui/src/components/AgentConfigForm.tsx` — create + edit Run Policy: checkbox plus start/end/timezone fields, shown only when interval heartbeats are enabled. Use existing form tokens only (no raw hex/px).
- `doc/spec/agent-runs.md` — add `activeHours` under §8.4 and note that the window applies only to `source=timer`.

Call sites (must stay consistent):

- `server/src/index.ts` — already calls `tickTimers`; no signature change.
- `ui/src/pages/NewAgent.tsx` / `ui/src/pages/NewAgent.test.tsx` — create path already builds runtimeConfig through the helpers above.
- `ui/src/pages/AgentDetail.tsx` — no new page; the existing config form is the editor. If the detail overview already prints heartbeat interval, show the window next to it when set.
- `packages/adapter-utils/src/types.ts` — add optional `activeHours` on `CreateConfigValues` only if the create form values type must carry the three fields; otherwise keep them on `runtimeConfig.heartbeat` only.

Tests:

- `packages/shared/src/active-hours.test.ts` — new. Inside window, before start, after end, overnight wrap, DST-safe timezone (`America/New_York`), invalid timezone rejected by schema, omitted window is always inside.
- `packages/shared/src/validators/status-card.test.ts` — still pass after the schema reuse.
- `packages/shared/src/validators/agent.ts` tests: add `packages/shared/src/validators/agent-runtime-config.test.ts` (new) for valid window, invalid timezone `422`/parse failure, and omitted window.
- `server/src/__tests__/heartbeat-timer-active-hours.test.ts` — new. Seed an enabled timer agent with a window; call `tickTimers` with a frozen `now` inside the window (enqueued or claimed) and outside the window (`skipped`, no new `heartbeat_runs` row, `lastHeartbeatAt` unchanged). Second case: comment/on-demand wake still enqueues outside the window. Third: missing `activeHours` still ticks.
- `server/src/__tests__/status-card-update-engine.test.ts` — existing active-hours cases still pass.
- `ui/src/lib/new-agent-runtime-config.test.ts` — omitted by default; present when the create form supplies it.
- `ui/src/lib/agent-config-patch.test.ts` — patch sets and clears `heartbeat.activeHours`.
- `ui/src/components/AgentConfigForm.test.ts` / `AgentConfigForm.render.test.tsx` — fields appear when heartbeat interval is on; hidden when off; timezone field is labeled.

## Test plan

1. `pnpm exec vitest run packages/shared/src/active-hours.test.ts packages/shared/src/validators/status-card.test.ts packages/shared/src/validators/agent-runtime-config.test.ts`
2. `pnpm exec vitest run server/src/__tests__/heartbeat-timer-active-hours.test.ts server/src/__tests__/status-card-update-engine.test.ts`
3. `pnpm exec vitest run ui/src/lib/new-agent-runtime-config.test.ts ui/src/lib/agent-config-patch.test.ts ui/src/components/AgentConfigForm.test.ts`
4. `pnpm --filter @paperclipai/shared typecheck && pnpm --filter @paperclipai/server typecheck && pnpm --filter @paperclipai/ui typecheck`
5. Manual: enable interval heartbeats, set 09:00–18:00 in a known timezone, confirm the next timer tick outside that window does not create a run, then ping the agent and confirm an on-demand run starts.

## Out of scope

- Company-wide default timezone or a company-level quiet-hours preset.
- Scheduling a specific issue to start at a clock time.
- Changing `maxDailyRuns` / `maxDailyCostCents` semantics.
- Telemetry events or run-log rows for skipped ticks.
