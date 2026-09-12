package com.redis.raft;

import java.util.List;

/**
 * RaftMessages — Phase 7's third file. Defines the ONLY two kinds of
 * request Raft nodes ever send each other, each with its own response
 * type — four small records total, grouped together in one file the same
 * way CommandProcessor.CommandResult (Phase 1) grouped a closely related
 * family of types in one place. No networking or logic lives here at all,
 * on purpose — these are pure DATA shapes; RaftNode.java (the next file)
 * is where the actual decision-making logic that produces and consumes
 * these lives, and a later transport file will be what actually sends
 * them over a socket.
 *
 * ===== The two RPCs Raft is built entirely out of =====
 * Real Raft (and this implementation) has exactly two kinds of message a
 * node ever sends to another node:
 *
 * 1. RequestVote — sent by a node trying to BECOME leader (a
 *    "candidate"), asking every other node in its group "will you vote
 *    for me in this election?"
 *
 * 2. AppendEntries — sent ONLY by the current leader, doing double duty
 *    as BOTH "please add these new log entries" AND, when its `entries`
 *    list is simply empty, a plain "heartbeat" — proof the leader is
 *    still alive, sent regularly to every follower so they don't time
 *    out and start an unnecessary new election. Using the exact same
 *    message shape for both real replication and heartbeats (rather than
 *    inventing a separate "I'm alive" message) is a deliberate real-Raft
 *    design choice we copy here: fewer message types to implement and
 *    reason about, and a heartbeat conveniently ALSO carries the
 *    leader's current term and commit index to every follower on every
 *    single beat.
 */
public class RaftMessages {

    /**
     * Sent by a candidate to every OTHER node in its group during an
     * election.
     *
     * `term` — the candidate's own current term (its election number —
     * see RaftNode.java for the full explanation of terms). A node
     * receiving this compares it against its own term to decide whether
     * this election is even worth considering.
     *
     * `candidateId` — which node is asking for this vote (we use each
     * node's configured string id, like "node1", the exact same kind of
     * id ClusterConfig.NodeInfo already uses in Phase 5).
     *
     * `lastLogIndex` / `lastLogTerm` — describe how up-to-date the
     * CANDIDATE'S OWN log is (RaftLog.lastIndex()/lastTerm() at the
     * moment it started this election). This is the heart of Raft's
     * safety guarantee: a node must REFUSE to vote for a candidate whose
     * log is less up-to-date than its own, which is exactly what
     * guarantees a newly elected leader is always guaranteed to already
     * hold every log entry any earlier majority ever agreed on.
     */
    public record RequestVoteRequest(long term, String candidateId, int lastLogIndex, long lastLogTerm) {
    }

    /**
     * The reply to a RequestVoteRequest.
     *
     * `term` — the RESPONDING node's own current term, which may be
     * HIGHER than the term the candidate asked with (e.g. if the
     * responder already knows about a newer election the candidate
     * hasn't heard about yet) — a candidate seeing a higher term back
     * always immediately abandons its own candidacy and reverts to being
     * a plain follower (see RaftNode.java).
     *
     * `voteGranted` — true only if the responder actually cast its one
     * vote (per term) for this specific candidate.
     */
    public record RequestVoteResponse(long term, boolean voteGranted) {
    }

    /**
     * Sent ONLY by a node that currently believes it's the leader —
     * either to replicate new entries, or (when `entries` is empty) as a
     * pure heartbeat.
     *
     * `term` — the leader's own current term.
     *
     * `leaderId` — which node is claiming to be leader (lets a follower
     * remember who to point clients toward, and lets a candidate that
     * receives this immediately recognize "oh, there's already a
     * legitimate leader for this term" and step down).
     *
     * `prevLogIndex` / `prevLogTerm` — describe the entry immediately
     * BEFORE the new ones being sent. The receiving follower checks: "do
     * I have an entry at prevLogIndex, and does ITS term match
     * prevLogTerm exactly?" Only if that check passes does the follower
     * accept the new entries — this is Raft's "log matching" safety
     * check, and it's what lets a leader safely assume that if a
     * follower accepts THIS append, that follower's log is now
     * IDENTICAL to the leader's up through the newly appended entries,
     * with no gaps or silent divergence possible.
     *
     * `entries` — the new LogEntry objects to append (empty for a plain
     * heartbeat with nothing new to replicate right now).
     *
     * `leaderCommit` — the highest log index the LEADER has confirmed is
     * committed (agreed on by a majority). A follower advances its own
     * "how far can I safely apply to my DataStore" marker to match this
     * (capped at its own log length, in case it hasn't caught up yet) —
     * this is how followers find out an entry is now safe to actually
     * apply, not just safe to store.
     */
    public record AppendEntriesRequest(long term, String leaderId, int prevLogIndex, long prevLogTerm,
                                        List<LogEntry> entries, int leaderCommit) {
    }

    /**
     * The reply to an AppendEntriesRequest.
     *
     * `term` — the responding node's own current term (same "a
     * responder can reveal a newer term than the sender knew about"
     * idea as RequestVoteResponse).
     *
     * `success` — true if the follower's log-matching check passed and
     * the entries (if any) were accepted; false if the check failed
     * (the follower's log at prevLogIndex doesn't match), telling the
     * leader it needs to retry further back in the log until it finds a
     * point both nodes agree on.
     */
    public record AppendEntriesResponse(long term, boolean success) {
    }
}
