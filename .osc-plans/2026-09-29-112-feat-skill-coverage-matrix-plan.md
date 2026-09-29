---
generated_by: osc-newfeature
bulk_run: true
type: feat
repo: paperclipai/paperclip
plan_id: 112
date: 2026-09-29
slug: skill-coverage-matrix
title: "feat: company skill coverage matrix"
feature_complexity: M
deep_discovery_report: true
deep_rubric_score: 8
dogfooded_general: false
video_required: false
PROCESS_LEVEL: PR_WELCOME
---

# feat: company skill coverage matrix

## Problem

Company skills are a library. Agent attachment is a separate desired-skill list on each agent. Operators can see `attachedAgentCount` on a skill card and a per-agent Skills tab, but there is no company-wide grid of who should have which installed skill. Skill detail `usedByAgents` only lists agents that already desire the skill, and it leaves `actualState` null on purpose so a detail read does not probe runtimes. After a team install or a later detach, gaps are silent.

## Behavior

Add a company-scoped coverage read and a Skills "Coverage" view.

`GET /api/companies/:companyId/skills/coverage`

Query:

- `q` — optional agent name/role or skill name/key filter
- `missingOnly` — when true, return only cells where the skill is installed in the company library and the agent does not desire it
- `skillKey` — optional single-skill slice
- `agentId` — optional single-agent slice

Response (company-scoped):

```json
{
  "skills": [
    { "id": "...", "key": "github-pr-workflow", "name": "GitHub PR workflow", "slug": "..." }
  ],
  "agents": [
    {
      "id": "...",
      "name": "CTO",
      "urlKey": "cto",
      "role": "engineer",
      "adapterType": "claude_local",
      "syncMode": "persistent"
    }
  ],
  "cells": [
    {
      "agentId": "...",
      "skillKey": "github-pr-workflow",
      "desired": true,
      "versionId": null,
      "actualState": null,
      "syncMode": "persistent"
    }
  ],
  "summary": {
    "agentCount": 12,
    "skillCount": 8,
    "desiredCellCount": 19,
    "gapCount": 5,
    "unsupportedAgentCount": 1
  }
}
```

Rules:

1. Company access and actor permission match `GET /companies/:companyId/skills`. Agents in the company may read coverage. They may not attach skills unless they already can call `POST /agents/:id/skills/sync`.
2. Register the route **before** `GET /companies/:companyId/skills/:skillId` so `coverage` is not parsed as a skill id.
3. Build the grid from the company skill library plus each agent's stored `paperclipSkillSync.desiredSkills`. Do **not** call adapter `listSkills` on this endpoint. `actualState` stays null on this read, same invariant as `usage()` on skill detail.
4. `syncMode` comes from the adapter's declared skill-sync capability (`unsupported` | `persistent` | `ephemeral`) without a live probe. Unsupported adapters still appear so the board can see desired rows that the runtime cannot honor.
5. Archived or terminated agents are omitted. Include paused agents (they still have a desired set).
6. Empty library or empty org returns empty arrays and zeroed summary, HTTP 200.
7. Board UI: add a Coverage view on the existing Skills page (`/skills?tab=coverage`). Show a filterable matrix (agents as rows, installed skills as columns, or the inverse on narrow viewports using the token layout — stack to a per-agent list of chips, no raw px). Each cell: desired / gap / unsupported. Selecting a gap opens the existing attach flow via `agentsApi.syncSkills(id, [...desired, skillKey], "add")`. No new mutation API.
8. Coverage is an operator view, not a hard assign-time gate. Do not block issue assignment when a cell is a gap.
9. Activity log only on attach (existing sync path). The GET writes nothing.
10. Do not add first-party telemetry events.

## Files

Implementation:

- `packages/shared/src/types/company-skill.ts` — add `CompanySkillCoverageQuery`, `CompanySkillCoverageSkill`, `CompanySkillCoverageAgent`, `CompanySkillCoverageCell`, `CompanySkillCoverageSummary`, `CompanySkillCoverageResponse`.
- `packages/shared/src/types/index.ts` — export the new types (next to `CompanySkillUsageAgent`).
- `packages/shared/src/validators/company-skill.ts` — `companySkillCoverageQuerySchema` and `companySkillCoverageResponseSchema`.
- `packages/shared/src/validators/index.ts` — export the schemas.
- `packages/shared/src/index.ts` — export types and schemas if this barrel is the package public surface.
- `server/src/services/company-skills.ts` — add `coverage(companyId, query)` that reuses `listReferenceTargets`, agent list, and `resolveDesiredSkillEntries`. Do not call `usage()` in a loop (that only returns desirers). Build all agents × installed skills in one pass.
- `server/src/routes/company-skills.ts` — `GET /companies/:companyId/skills/coverage` with `validate(companySkillCoverageQuerySchema)` and `assertCompanyAccess`. Place the route above `/:skillId`.
- `ui/src/api/companySkills.ts` — `coverage(companyId, query)`.
- `ui/src/pages/skills/skills-navigation.ts` — extend `SkillsNavigationView` with `"coverage"` and `SKILLS_NAVIGATION_HREFS.coverage = "/skills?tab=coverage"`. `resolveSkillsDiscoveryView` stays Installed/Discover; `resolveSkillsNavigationView` returns `coverage` when `tab=coverage`.
- `ui/src/pages/skills/SkillCoverageMatrix.tsx` — new presentational + data page section: summary counts, search, missing-only toggle, matrix/list, attach action using `agentsApi.syncSkills`.
- `ui/src/pages/CompanySkills.tsx` — add the Coverage tab trigger and render `SkillCoverageMatrix` when the nav view is coverage.
- `ui/src/pages/CompanySkills.production.tsx` — same tab wiring (this tree is a source twin, not a generated artifact).
- `skills/paperclip/references/company-skills.md` — document the coverage GET and that it is desired-state only (no live adapter probe).
- `doc/plans/2026-03-14-skills-ui-product-plan.md` — mark the multi-agent desired-vs-actual library view as shipped for **desired** state; leave live `actualState` probing as a later follow-up so this PR stays honest.

Call sites:

- `ui/src/pages/agent-skills/AgentSkillsTab.tsx` — optional one-line link to `/skills?tab=coverage` from the agent Skills tab; not required for the feature to work.
- `ui/src/lib/company-skill-routes.ts` — no change unless coverage needs a canonical href helper; prefer `SKILLS_NAVIGATION_HREFS`.
- `server/src/routes/index.ts` — no change; `companySkillRoutes` already mounted.
- Existing `POST /api/agents/:id/skills/sync` (`server/src/routes/agents.ts`, `ui/src/api/agents.ts`) is the only write path.

Tests:

- `packages/shared/src/validators/company-skill.test.ts` — query defaults, `missingOnly` coercion, response schema accepts null `actualState`.
- `server/src/__tests__/company-skills-service.test.ts` — coverage: two agents, two installed skills, one desired cell, summary.gapCount; terminated agent omitted; `missingOnly` filters; no adapter `listSkills` mock invoked.
- `server/src/__tests__/company-skills-routes.test.ts` — `GET .../skills/coverage` 200 with company access, 404/403 without; `coverage` is not captured by `/:skillId`.
- `ui/src/pages/skills/skills-navigation.test.ts` — `tab=coverage` resolves to coverage; Installed/Discover cases unchanged.
- `ui/src/pages/skills/SkillCoverageMatrix.test.tsx` — new. Renders gap/desired cells from fixture payload; missing-only hides desired cells; attach calls `syncSkills` with mode `add`.
- `ui/src/pages/CompanySkills.test.tsx` — Coverage tab is reachable and mounts the matrix.

## Test plan

1. `pnpm exec vitest run packages/shared/src/validators/company-skill.test.ts`
2. `pnpm exec vitest run server/src/__tests__/company-skills-service.test.ts server/src/__tests__/company-skills-routes.test.ts`
3. `pnpm exec vitest run ui/src/pages/skills/skills-navigation.test.ts ui/src/pages/skills/SkillCoverageMatrix.test.tsx ui/src/pages/CompanySkills.test.tsx`
4. `pnpm --filter @paperclipai/shared typecheck && pnpm --filter @paperclipai/server typecheck && pnpm --filter @paperclipai/ui typecheck`
5. `pnpm check:token-gates` after the UI files change.
6. Manual: install two company skills, attach one to one agent, open `/skills?tab=coverage`, confirm the gap cell, attach from the cell, confirm the Skills tab and skill detail `usedByAgents` update.

## Out of scope

- Live adapter probing or filling `actualState` on this endpoint.
- Hard-blocking assignment when a required skill is missing.
- Folder-level or role-level required-skill policy.
- Changing `GET /agents/:id/skills` or skill-detail `usage()`.
