/**
 * @module convex/crons
 *
 * Purpose:
 * Registers scheduled Convex cron jobs for maintenance tasks.
 *
 * Behavior:
 * - Runs rate-limit cleanup daily at 03:00 UTC
 *
 * Constraints:
 * - Only `internal.*` functions are scheduled here.
 * - Cron frequency is a deployment-level decision; these are safe defaults.
 *
 * Non-Goals:
 * - Tenant-specific scheduling policy.
 * - Runtime feature flags (handled via deployment configuration).
 */
import { cronJobs } from 'convex/server';
import { internal } from './_generated/api';

const crons = cronJobs();

// Run rate limit cleanup daily at 3:00 UTC
crons.daily('gc:rate-limits', { hourUTC: 3, minuteUTC: 0 }, internal.rateLimits.cleanup);

export default crons;
