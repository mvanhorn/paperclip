import { execFileSync, type ExecFileSyncOptions } from "node:child_process";

export function execGitSync(args: string[], options: ExecFileSyncOptions = {}) {
  return execFileSync("git", args, {
    ...options,
    env: {
      ...process.env,
      GIT_CONFIG_GLOBAL: "/dev/null",
      GIT_CONFIG_SYSTEM: "/dev/null",
      ...(options.env ?? {}),
    },
  });
}
