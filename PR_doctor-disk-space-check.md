Title: feat(cli): warn in doctor when Paperclip data volumes run low on free space

## Thinking Path

> - Paperclip is the open source app people use to manage AI agents for work
> - A local instance stores embedded Postgres data, file storage, logs, and database backups under the instance home
> - Those directories grow during normal use and can fill the volume
> - When the volume is full, embedded Postgres rejects writes and backups fail, and the operator sees runs stop without a clear cause
> - Operators already run `paperclipai doctor` for config, storage, database, logs, and related health
> - This pull request adds a read-only disk space check to doctor
> - The benefit is an early warning on the same volumes Paperclip uses, without blocking `paperclipai run`

## Linked Issues or Issue Description

**Subsystem affected**

cli/ — `paperclipai doctor` diagnostics

**Problem or motivation**

A local Paperclip instance keeps embedded Postgres data, local file storage, logs, and database backups on disk. Those paths grow with use. When the volume fills, embedded Postgres returns `No space left on device`, backups fail, and runs stop. Doctor did not report free space on those volumes.

**Proposed solution**

Add a doctor check named Disk space. Measure free space with `statfs` on the instance data, storage, log, backup, and home paths. Group paths that share a filesystem. Warn when free space is below the smaller of 5 GiB and 10% of the volume. Below 512 MiB free, say that embedded Postgres may stop accepting writes. Never fail. `paperclipai run` already starts when doctor only warns.

**Alternatives considered**

A server-side disk monitor and Postgres supervisor would recover after the disk is already full. That work is larger and is not this change. A doctor `fail` status would block `paperclipai run`, which already treats warnings as non-blocking.

**Roadmap alignment**

This change does not overlap a named item in `ROADMAP.md`. It extends existing local-instance doctor diagnostics.

Refs #6622
Refs #7587

## What Changed

- Added `cli/src/checks/disk-space-check.ts` and exported it from `cli/src/checks/index.ts`
- Doctor now runs the Disk space check after the log directory check
- The check measures embedded Postgres data (embedded mode only), local disk storage, logs, the backup directory, and instance home
- Paths on the same filesystem are reported once; missing paths use the nearest existing ancestor
- Status is `pass` or `warn` only. `statfs` errors become warnings
- Documented the check and thresholds in `doc/CLI.md` and `docs/cli/setup-commands.md`
- Added unit tests for pass, 10% and 5 GiB edges, 512 MiB wording, grouping, ancestor measurement, external Postgres, and `statfs` errors
- Extended doctor tests so step order and summary counts include Disk space

## Verification

Focused CLI tests:

```
pnpm vitest run cli/src/__tests__/disk-space-check.test.ts cli/src/__tests__/doctor.test.ts
```

Result: 2 files, 12 tests passed.

```
pnpm vitest run cli/src/__tests__/database-check.test.ts
```

Result: 1 file, 3 tests passed.

```
pnpm --filter paperclipai typecheck
```

Result: pass.

```
pnpm --filter paperclipai build
```

Result: pass.

```
pnpm verify
```

Result: exit 2. Local verify asked for broader CI because `doc/CLI.md` changed. Focused tests above passed.

Manual demo on this VM (sudo available):

1. `pnpm paperclipai doctor --data-dir /tmp/paperclip-doctor-demo --config ... --repair --yes`
   Disk space: `242.1 GiB free (96%) on / for data, storage, logs, backups, instance`. Summary: `13 passed`.
2. Loop-mounted a 2048 MiB ext4 image at `/mnt/paperclip-low`. After `fallocate`, `df -h` showed `100M` available (`104853504` bytes).
3. Set `database.backup.dir` to `/mnt/paperclip-low/backups` and ran doctor again.
   Disk space: `99.9 MiB free (5%) on /mnt/paperclip-low (backups). Less than 194.5 MiB free (10% of volume size). embedded Postgres may stop accepting writes`. Repair hint names `paperclipai configure --section database`. Summary: `12 passed, 1 warnings`.
4. `paperclipai run` still started. Doctor printed the same warning, then `Starting Paperclip server...`, `Embedded PostgreSQL ready`, and `Server listener bound on 127.0.0.1:33111`. `run.ts` only stops the server when `summary.failed > 0`.

Not verified: `statfs` on network filesystems, Windows, and macOS APFS.

## Risks

Low risk. The check is read-only and never returns `fail`, so `paperclipai run` still starts after a warning. `statfs` `bavail` can differ from `df` on network filesystems or volumes with reserved blocks. A `statfs` error becomes a warning and does not throw. The backup-directory repair hint points at `paperclipai configure --section database`.

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
- [x] I have run tests locally and they pass
- [x] I have added or updated tests where applicable
- [x] I have updated relevant documentation to reflect my changes
- [x] I have considered and documented any risks above
- [ ] All Paperclip CI gates are green
- [ ] Greptile is 5/5 with no open P2s, recommendations, or follow-ups
- [ ] I will address all Greptile and reviewer comments before requesting merge

## Demo

![demo](https://raw.githubusercontent.com/mvanhorn/pr-demo-assets/main/osc-bulk-1115b/doctor-disk-space-check/preview.gif)

<video src="https://raw.githubusercontent.com/mvanhorn/pr-demo-assets/main/osc-bulk-1115b/doctor-disk-space-check/out.mp4" controls width="720"></video>
