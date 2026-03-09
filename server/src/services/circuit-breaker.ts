import { and, desc, eq } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { agents, heartbeatRuns } from "@paperclipai/db";
import { logger } from "../middleware/logger.js";
import { logActivity } from "./activity-log.js";
import { publishLiveEvent } from "./live-events.js";
import { parseObject, asNumber, asBoolean } from "../adapters/utils.js";

export interface CircuitBreakerConfig {
  enabled: boolean;
  maxConsecutiveFailures: number;
  maxConsecutiveNoProgress: number;
  tokenVelocityMultiplier: number;
}

const DEFAULTS: CircuitBreakerConfig = {
  enabled: true,
  maxConsecutiveFailures: 3,
  maxConsecutiveNoProgress: 5,
  tokenVelocityMultiplier: 3.0,
};

export function parseCircuitBreakerConfig(agent: typeof agents.$inferSelect): CircuitBreakerConfig {
  const runtimeConfig = parseObject(agent.runtimeConfig);
  const cb = parseObject(runtimeConfig.circuitBreaker);

  return {
    enabled: asBoolean(cb.enabled, DEFAULTS.enabled),
    maxConsecutiveFailures: Math.max(1, asNumber(cb.maxConsecutiveFailures, DEFAULTS.maxConsecutiveFailures)),
    maxConsecutiveNoProgress: Math.max(1, asNumber(cb.maxConsecutiveNoProgress, DEFAULTS.maxConsecutiveNoProgress)),
    tokenVelocityMultiplier: Math.max(1.5, asNumber(cb.tokenVelocityMultiplier, DEFAULTS.tokenVelocityMultiplier)),
  };
}

export type TripReason = "consecutive_failures" | "consecutive_no_progress" | "token_velocity_spike";

export interface CircuitBreakerResult {
  tripped: boolean;
  reason?: TripReason;
  details?: Record<string, unknown>;
}

/**
 * Evaluate circuit breaker conditions for an agent after a run completes.
 * Returns whether the breaker should trip and why.
 */
export async function evaluateCircuitBreaker(
  db: Db,
  agentId: string,
  outcome: "succeeded" | "failed" | "cancelled" | "timed_out",
): Promise<CircuitBreakerResult> {
  const agent = await db
    .select()
    .from(agents)
    .where(eq(agents.id, agentId))
    .then((rows) => rows[0] ?? null);

  if (!agent) return { tripped: false };

  const config = parseCircuitBreakerConfig(agent);
  if (!config.enabled) return { tripped: false };

  // Skip evaluation for cancelled runs - those are intentional
  if (outcome === "cancelled") return { tripped: false };

  // Check consecutive failures
  if (outcome === "failed" || outcome === "timed_out") {
    const failureResult = await checkConsecutiveFailures(db, agentId, config);
    if (failureResult.tripped) return failureResult;
  }

  // Check consecutive no-progress (only for succeeded runs that did nothing useful)
  if (outcome === "succeeded") {
    const noProgressResult = await checkConsecutiveNoProgress(db, agentId, config);
    if (noProgressResult.tripped) return noProgressResult;
  }

  // Check token velocity spike
  const velocityResult = await checkTokenVelocity(db, agentId, config);
  if (velocityResult.tripped) return velocityResult;

  return { tripped: false };
}

async function checkConsecutiveFailures(
  db: Db,
  agentId: string,
  config: CircuitBreakerConfig,
): Promise<CircuitBreakerResult> {
  const recentRuns = await db
    .select({ status: heartbeatRuns.status })
    .from(heartbeatRuns)
    .where(eq(heartbeatRuns.agentId, agentId))
    .orderBy(desc(heartbeatRuns.createdAt))
    .limit(config.maxConsecutiveFailures);

  if (recentRuns.length < config.maxConsecutiveFailures) return { tripped: false };

  const allFailed = recentRuns.every(
    (r) => r.status === "failed" || r.status === "timed_out",
  );

  if (allFailed) {
    return {
      tripped: true,
      reason: "consecutive_failures",
      details: {
        consecutiveFailures: config.maxConsecutiveFailures,
        threshold: config.maxConsecutiveFailures,
      },
    };
  }

  return { tripped: false };
}

async function checkConsecutiveNoProgress(
  db: Db,
  agentId: string,
  config: CircuitBreakerConfig,
): Promise<CircuitBreakerResult> {
  // A run with no progress means it succeeded but produced no meaningful output.
  // We detect this by checking if recent succeeded runs have zero token output
  // (empty runs) or if the resultJson indicates no actions taken.
  const recentRuns = await db
    .select({
      status: heartbeatRuns.status,
      usageJson: heartbeatRuns.usageJson,
      resultJson: heartbeatRuns.resultJson,
    })
    .from(heartbeatRuns)
    .where(eq(heartbeatRuns.agentId, agentId))
    .orderBy(desc(heartbeatRuns.createdAt))
    .limit(config.maxConsecutiveNoProgress);

  if (recentRuns.length < config.maxConsecutiveNoProgress) return { tripped: false };

  // All recent runs must be succeeded (failures are caught separately)
  const allSucceeded = recentRuns.every((r) => r.status === "succeeded");
  if (!allSucceeded) return { tripped: false };

  // Check if all runs show no meaningful progress.
  // A run shows no progress if it has no resultJson actions
  // OR if resultJson.issuesModified is 0/missing
  const allNoProgress = recentRuns.every((r) => {
    const result = r.resultJson as Record<string, unknown> | null;
    if (!result) return true;
    const issuesModified = Number(result.issuesModified ?? result.issuesMoved ?? 0);
    const issuesCreated = Number(result.issuesCreated ?? 0);
    const commentsPosted = Number(result.commentsPosted ?? 0);
    return issuesModified === 0 && issuesCreated === 0 && commentsPosted === 0;
  });

  if (allNoProgress) {
    return {
      tripped: true,
      reason: "consecutive_no_progress",
      details: {
        consecutiveNoProgress: config.maxConsecutiveNoProgress,
        threshold: config.maxConsecutiveNoProgress,
      },
    };
  }

  return { tripped: false };
}

async function checkTokenVelocity(
  db: Db,
  agentId: string,
  config: CircuitBreakerConfig,
): Promise<CircuitBreakerResult> {
  // Get the last 20 runs to compute a rolling average
  const recentRuns = await db
    .select({ usageJson: heartbeatRuns.usageJson })
    .from(heartbeatRuns)
    .where(
      and(
        eq(heartbeatRuns.agentId, agentId),
        eq(heartbeatRuns.status, "succeeded"),
      ),
    )
    .orderBy(desc(heartbeatRuns.createdAt))
    .limit(20);

  // Need at least 5 data points for a meaningful average
  if (recentRuns.length < 5) return { tripped: false };

  const costs = recentRuns.map((r) => {
    const usage = r.usageJson as Record<string, unknown> | null;
    if (!usage) return 0;
    const costUsd = Number(usage.costUsd ?? 0);
    return Math.round(costUsd * 100); // cents
  });

  const latestCost = costs[0];
  if (latestCost === 0) return { tripped: false };

  // Average of all runs except the latest
  const historicalCosts = costs.slice(1);
  const avgCost = historicalCosts.reduce((sum, c) => sum + c, 0) / historicalCosts.length;

  if (avgCost === 0) return { tripped: false };

  const ratio = latestCost / avgCost;

  if (ratio >= config.tokenVelocityMultiplier) {
    return {
      tripped: true,
      reason: "token_velocity_spike",
      details: {
        latestCostCents: latestCost,
        averageCostCents: Math.round(avgCost),
        ratio: Number(ratio.toFixed(2)),
        threshold: config.tokenVelocityMultiplier,
      },
    };
  }

  return { tripped: false };
}

/**
 * Trip the circuit breaker: pause the agent, log activity, publish event.
 */
export async function tripCircuitBreaker(
  db: Db,
  agentId: string,
  result: CircuitBreakerResult,
) {
  const agent = await db
    .select()
    .from(agents)
    .where(eq(agents.id, agentId))
    .then((rows) => rows[0] ?? null);

  if (!agent) return;

  // Don't trip if already paused/terminated
  if (agent.status === "paused" || agent.status === "terminated") return;

  logger.warn(
    { agentId, agentName: agent.name, reason: result.reason, details: result.details },
    "circuit breaker tripped - pausing agent",
  );

  await db
    .update(agents)
    .set({ status: "paused", updatedAt: new Date() })
    .where(eq(agents.id, agentId));

  await logActivity(db, {
    companyId: agent.companyId,
    actorType: "system",
    actorId: "circuit-breaker",
    action: "agent.circuit_breaker_tripped",
    entityType: "agent",
    entityId: agentId,
    agentId,
    details: {
      reason: result.reason,
      ...result.details,
    },
  });

  publishLiveEvent({
    companyId: agent.companyId,
    type: "agent.status",
    payload: {
      agentId: agent.id,
      status: "paused",
      reason: "circuit_breaker_tripped",
      circuitBreakerReason: result.reason,
      circuitBreakerDetails: result.details,
    },
  });
}
