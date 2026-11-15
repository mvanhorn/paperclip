# Demo: doctor disk space check

Sudo is available on this VM. This demo uses a real 2048 MiB loop-mounted ext4 filesystem.

## Healthy instance

Throwaway home: `/tmp/paperclip-doctor-demo`.

```
pnpm paperclipai doctor --data-dir /tmp/paperclip-doctor-demo --config /tmp/paperclip-doctor-demo/instances/default/config.json --repair --yes
```

Disk space pass line:

```
✓ Disk space: 242.1 GiB free (96%) on / for data, storage, logs, backups, instance
```

Summary: `13 passed`. Exit code 0.

Host volume next to that line:

```
df -h /
/dev/vdc        254G   12G  243G   5% /
```

## Low-space volume

Created `/tmp/paperclip-low-disk.img` (2048 MiB), formatted ext4 with reserved blocks disabled, and mounted at `/mnt/paperclip-low`.

Before fill:

```
/dev/loop0      2.0G   28K  1.9G   1% /mnt/paperclip-low
```

Filled with `fallocate -l 1918709760 /mnt/paperclip-low/fill.bin`. After fill:

```
/dev/loop0      2.0G  1.8G  100M  95% /mnt/paperclip-low
1B-blocks 2040373248  used 1918742528  available 104853504
```

Pointed `database.backup.dir` at `/mnt/paperclip-low/backups` and ran doctor again.

Disk space warn line:

```
! Disk space: 99.9 MiB free (5%) on /mnt/paperclip-low (backups). Less than 194.5 MiB free (10% of volume size). embedded Postgres may stop accepting writes; 242.1 GiB free (96%) on / for data, storage, logs, instance
```

Repair hint:

```
Free disk space on the affected volume, or move the backup directory with `paperclipai configure --section database`
```

Summary: `12 passed, 1 warnings`. Outro: `All critical checks passed with some warnings.` Exit code 0.

The 99.9 MiB free figure is below 10% of the 2.0 GiB volume (threshold 194.5 MiB) and below 512 MiB, so both wordings appear. `df -h` shows 100M available on the same mount.

## `paperclipai run` does not block on warn

`cli/src/commands/run.ts` exits only when `summary.failed > 0`. A disk space warning is not a failure.

Real start with the low-space backup directory:

```
timeout --signal=INT 45s pnpm paperclipai run --data-dir /tmp/paperclip-doctor-demo --config ... --force
```

Doctor still reported `12 passed, 1 warnings`, then:

```
Starting Paperclip server...
Embedded PostgreSQL ready
Server listener bound on 127.0.0.1:33111; startup recovery in progress
```

The process received SIGINT after 45s (`run_exit=124`). Doctor did not stop the start.

## Media

- `out.mp4`: HyperFrames replay of `commands.log`, 1920x1080, 30 fps, 720 frames, 24.0 s, H.264 yuv420p
- `preview.gif`: 960x540, 12 fps, 24.0 s, 3.4 MiB
