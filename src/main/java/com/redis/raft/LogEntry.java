package com.redis.raft;

import java.util.List;

/**
 * LogEntry — Phase 7's first file. The smallest building block of Raft: one
 * single entry in a Raft group's replicated log.
 *
 * ===== Why Raft needs a REPLICATED LOG at all (the big picture) =====
 * In Phase 6, a primary just applied a write directly to its own DataStore
 * and then pushed the command to its replica — simple, but with no way to
 * survive the primary itself dying: nothing else in that 2-node setup could
 * ever safely take over, because a lone survivor can never prove to itself
 * (or to a client) that it has every write the old primary might have
 * accepted right before crashing.
 *
 * Raft's fix: every write first becomes a LogEntry, appended to an
 * identical, ordered log kept by every node in the group. A write is only
 * considered "durable and official" once a MAJORITY of the group's nodes
 * have that exact entry in their log at the exact same position — this
 * majority requirement is what makes the whole approach safe even if any
 * MINORITY of nodes (including the current leader) crashes at any moment:
 * whichever nodes get elected leader afterward are mathematically
 * guaranteed (by Raft's election rules, in a later file) to already have
 * every entry a majority agreed on. A LogEntry is the one unit everything
 * else in this phase — voting, leader election, log replication — is built
 * around.
 *
 * ===== Fields =====
 * `term` — WHEN, in Raft's own logical clock, this entry was created (see
 * RaftNode.java, a later file, for what a "term" fully means — for now,
 * think of it as a monotonically increasing "election number": every time
 * a new leader gets elected, the term increases). Raft uses each entry's
 * term to detect and resolve conflicts between logs that disagree with
 * each other after a leader change.
 *
 * `command` — the actual Redis command this entry represents, stored in
 * the EXACT SAME shape (`List<String>`, command name first) that
 * CommandProcessor.process() already expects everywhere else in this
 * project — e.g. `["SET", "foo", "bar"]`. Once an entry is "committed"
 * (later file), applying it to DataStore is just
 * `commandProcessor.process(entry.command())` — no new command
 * representation needed anywhere.
 *
 * This is a `record` (same reasoning as ClusterConfig.NodeInfo in Phase 5:
 * a plain, immutable bundle of a few named values, nothing more) —
 * `term()` and `command()` are its auto-generated accessors.
 */
public record LogEntry(long term, List<String> command) {
}
