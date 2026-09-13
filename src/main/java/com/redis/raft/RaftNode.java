package com.redis.raft;

import com.redis.core.CommandProcessor;
import com.redis.persistence.WriteAheadLog;
import com.redis.raft.RaftMessages.AppendEntriesRequest;
import com.redis.raft.RaftMessages.AppendEntriesResponse;
import com.redis.raft.RaftMessages.RequestVoteRequest;
import com.redis.raft.RaftMessages.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RaftNode — Phase 7's fourth file, and the heart of the whole phase: the
 * actual Raft state machine. Deliberately built with ZERO networking or
 * timers inside it — exactly the same "pure logic first" philosophy this
 * project has followed since RESPParser (Phase 3) and ClusterConfig (Phase
 * 5): every method here is a plain, synchronous, in-memory state
 * transition that a unit test can exercise directly, with no socket, no
 * thread, no sleep, anywhere in this file. A later file (RaftTransport)
 * will handle actually sending these RPCs over the network and running
 * election-timeout/heartbeat timers, calling into this class's methods to
 * do the real thinking.
 *
 * ===== Terms, in plain English =====
 * A "term" is Raft's own logical clock — just a number that only ever goes
 * UP, never down. Every time a new election starts, the term increases by
 * one. At most ONE node can be leader for any given term (that's what the
 * election guarantees). Whenever any node sees a message with a HIGHER
 * term than its own, that's an unconditional signal "something newer has
 * happened that I don't know about yet" — it immediately adopts that
 * higher term and reverts to being a plain FOLLOWER, no matter what it
 * was doing a moment before (even if it currently thinks IT is the
 * leader). This one rule is what keeps the whole cluster converging on a
 * single, agreed table of "who's in charge right now," even after
 * arbitrary crashes and network hiccups.
 *
 * ===== The three roles =====
 * FOLLOWER — the default, passive role. Just waits for AppendEntries
 *   (heartbeats/replication) from a leader, or times out and becomes a
 *   CANDIDATE if it hasn't heard from one in too long.
 * CANDIDATE — actively trying to become leader: voted for itself, asked
 *   every peer for a vote, waiting to see if it gets a majority.
 * LEADER — the one node currently accepting new writes and replicating
 *   them to everyone else, via a continuous stream of AppendEntries.
 */
public class RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final String selfId;
    private final List<String> peerIds; // every OTHER node in this Raft group (not including selfId)
    private final RaftLog log;
    private final CommandProcessor commandProcessor; // applies committed entries to this node's DataStore

    // ===== Phase 7 client-wiring additions =====
    // wal: this node's OWN local Write-Ahead Log (Phase 2). Note WHERE
    // we log now, compared to Phase 1-6: back then, ClientHandler logged
    // a write to the WAL BEFORE calling process() on it directly. Now,
    // for a Raft-managed shard, a command isn't safe to treat as
    // "happened" until it's COMMITTED (agreed by a majority) - logging
    // it any earlier (e.g. the moment a leader merely receives it) could
    // durably record something that a later leader's conflicting entry
    // ends up truncating away, which would be a genuine correctness bug.
    // So the WAL write now happens right here, in applyCommittedEntries,
    // at the exact same moment we apply the command to DataStore - the
    // same "log immediately before apply" invariant Phase 2 established,
    // just moved to run at COMMIT time instead of RECEIPT time. This also
    // means FOLLOWERS now get their own correct local WAL too (something
    // Phase 6's ReplicaClient used to handle for replicas, but nothing
    // was doing for Raft followers until now).
    private final WriteAheadLog wal;

    // appliedResults: remembers the CommandResult produced by applying
    // each log index, so the client-facing networking layer (a leader
    // that just called proposeCommand and is now WAITING for that
    // specific index to commit) can retrieve the real result once it's
    // ready, rather than having to re-run the command a second time
    // (which would be actively wrong for something like an incrementing
    // counter). We only ever need to look a FEW indexes back at once (one
    // per currently-waiting client), so we periodically trim old entries
    // below to stop this map from growing forever.
    private final Map<Integer, CommandProcessor.CommandResult> appliedResults = new HashMap<>();

    // ===== Persistent-in-spirit Raft state =====
    // Real Raft requires these three to be written to disk BEFORE replying
    // to any RPC that changes them, so a crash-and-restart never "forgets"
    // a vote it already cast or a term it already saw (which could
    // otherwise let it vote twice in the same term after restarting — a
    // real safety violation). We're keeping this in-memory only for now,
    // a known, explicitly-flagged simplification appropriate for this
    // project's scope (a crashed node simply starts fresh as a brand-new
    // follower in the group's current term, which it will quickly learn
    // about from the next heartbeat it receives) — persisting this
    // properly would be a reasonable Phase 10 hardening item.
    private long currentTerm = 0;
    private String votedFor = null;

    private volatile Role role = Role.FOLLOWER;

    // Tracks who this node last heard claim to be a legitimate leader —
    // useful later (client-facing wiring) so a follower can tell a client
    // "I'm not the leader, try THIS node instead" rather than just "I
    // don't know."
    private volatile String leaderId = null;

    // The highest log index known to be committed (agreed on by a
    // majority) — entries up to here are safe to apply to DataStore.
    private int commitIndex = 0;
    // The highest log index actually applied to DataStore so far.
    private int lastApplied = 0;

    // ===== LEADER-only state (reset fresh every time this node becomes leader) =====
    // For each peer: the NEXT log index we believe we need to send them.
    // Starts optimistically at our own log's end + 1 (assuming they're
    // fully caught up) and gets walked backward whenever they reject an
    // AppendEntries, until we find a point our logs actually agree on.
    private Map<String, Integer> nextIndex;
    // For each peer: the highest log index we've CONFIRMED (via a
    // successful AppendEntries response) they actually have. Used to
    // compute the real replication majority for advancing commitIndex.
    private Map<String, Integer> matchIndex;

    // Vote-counting state for an election THIS node is currently running
    // as a candidate. Reset every time a new election starts.
    private long electionTerm = 0;
    private int votesReceived = 0;

    // Updated whenever this node has a good reason to NOT start a new
    // election right now: hearing a valid heartbeat/AppendEntries from a
    // current leader, granting a vote to a candidate, or just having
    // started its own election. The timer-driving file (RaftTransport)
    // reads this to decide whether an election timeout has elapsed.
    private volatile long lastResetAt = System.currentTimeMillis();

    /**
     * Test-friendly constructor (no WAL) — used by RaftNodeTest, which
     * builds these objects entirely in-memory with no real files
     * involved. Mirrors the exact same "two-constructor, null means off"
     * pattern CommandProcessor already uses for its own optional
     * ClusterConfig — here, wal == null simply means "don't bother
     * logging applied entries," which is fine for a throwaway test node
     * that never restarts and needs no crash recovery.
     */
    public RaftNode(String selfId, List<String> peerIds, RaftLog log, CommandProcessor commandProcessor) {
        this(selfId, peerIds, log, commandProcessor, null);
    }

    public RaftNode(String selfId, List<String> peerIds, RaftLog log, CommandProcessor commandProcessor,
                     WriteAheadLog wal) {
        this.selfId = selfId;
        this.peerIds = peerIds;
        this.log = log;
        this.commandProcessor = commandProcessor;
        this.wal = wal;
    }

    // ==================== Simple state accessors ====================

    // Phase 8 addition: the dashboard needs to label each node by its own
    // id when displaying status - selfId itself never changes after
    // construction, so this needs no synchronization at all (unlike the
    // fields below it, which mutate as the node's role/term change).
    public String getSelfId() {
        return selfId;
    }

    public synchronized Role getRole() {
        return role;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getLastResetAt() {
        return lastResetAt;
    }

    private void resetElectionTimer() {
        lastResetAt = System.currentTimeMillis();
    }

    // ==================== Handling an incoming RequestVote ====================

    /**
     * Called (by a later networking file) whenever this node RECEIVES a
     * RequestVoteRequest from some candidate. Decides whether to grant
     * our vote, following Raft's exact voting rules.
     */
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        // Rule 1: ANY message carrying a higher term than ours means we're
        // behind — adopt it immediately and become a follower, BEFORE
        // doing anything else with this request.
        if (request.term() > currentTerm) {
            stepDownToFollower(request.term());
        }

        // Rule 2: never vote for a stale election. If the candidate's
        // term is behind ours, flatly refuse and tell it our (newer)
        // term, so it can catch up.
        if (request.term() < currentTerm) {
            return new RequestVoteResponse(currentTerm, false);
        }

        // Rule 3: we can only grant a vote if we haven't already voted
        // for someone ELSE this term (voting for the SAME candidate
        // again, e.g. a retried request, is fine and idempotent).
        boolean notYetVotedThisTerm = (votedFor == null || votedFor.equals(request.candidateId()));

        // Rule 4: the candidate's log must be AT LEAST AS UP-TO-DATE as
        // ours. "Up-to-date" compares by TERM first (a higher last-log
        // term wins outright), and only falls back to comparing INDEX
        // when both logs' last terms are equal (in which case, the
        // longer log wins). This is Raft's core safety mechanism: it
        // guarantees a node whose log is missing entries a majority
        // already agreed on can never win an election, because a
        // majority of voters would see their own log as more up-to-date
        // and refuse.
        boolean candidateLogIsUpToDate;
        long ourLastTerm = log.lastTerm();
        int ourLastIndex = log.lastIndex();
        if (request.lastLogTerm() != ourLastTerm) {
            candidateLogIsUpToDate = request.lastLogTerm() > ourLastTerm;
        } else {
            candidateLogIsUpToDate = request.lastLogIndex() >= ourLastIndex;
        }

        if (notYetVotedThisTerm && candidateLogIsUpToDate) {
            votedFor = request.candidateId();
            resetElectionTimer(); // granting a vote is a reason to not also start our own election right now
            logger.info("Voted for {} in term {}", request.candidateId(), currentTerm);
            return new RequestVoteResponse(currentTerm, true);
        }

        return new RequestVoteResponse(currentTerm, false);
    }

    // ==================== Handling an incoming AppendEntries ====================

    /**
     * Called whenever this node RECEIVES an AppendEntries RPC (whether
     * it's carrying real new entries, or is just an empty heartbeat) from
     * a node claiming to be the leader.
     */
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        if (request.term() > currentTerm) {
            stepDownToFollower(request.term());
        }

        // A leader from an OLDER term than ours is illegitimate (there's
        // already been a newer election since) — reject outright and let
        // it learn our newer term so it can step down itself.
        if (request.term() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false);
        }

        // At this point, request.term() == currentTerm, and its sender is
        // a LEGITIMATE current leader (there can only be one leader per
        // term, by construction) — even if WE were a candidate ourselves
        // a moment ago, seeing this means we lost (or never entered) this
        // election; accept reality and become a follower under this
        // leader.
        role = Role.FOLLOWER;
        leaderId = request.leaderId();
        resetElectionTimer();

        // ===== Raft's log-matching check =====
        // "Do I even HAVE an entry at prevLogIndex, and if so, does ITS
        // term match exactly what the leader says it should be?" A
        // mismatch here means our log has diverged from the leader's at
        // or before this point (perhaps from an earlier, now-abandoned
        // leader) — we must reject so the leader knows to walk further
        // back and find where our logs actually still agree.
        // prevLogIndex == 0 is a special case meaning "there's nothing
        // before this at all" (we're at the very start of the log), which
        // trivially always matches.
        if (request.prevLogIndex() > 0) {
            long ourTermAtPrev = log.termAt(request.prevLogIndex());
            if (ourTermAtPrev != request.prevLogTerm()) {
                return new AppendEntriesResponse(currentTerm, false);
            }
        }

        // ===== Append the new entries (with conflict resolution) =====
        // Walk through each incoming entry in order. The moment we find
        // one of OUR existing entries at the same position with a
        // DIFFERENT term, that's a genuine conflict (leftover from an
        // old, abandoned leader) — truncate our log from exactly that
        // point onward, then append this and every entry after it fresh
        // from the leader's version.
        int index = request.prevLogIndex();
        for (LogEntry entry : request.entries()) {
            index++;
            long ourTermAtIndex = log.termAt(index);
            if (ourTermAtIndex != 0 && ourTermAtIndex != entry.term()) {
                log.truncateFrom(index);
                log.append(entry);
            } else if (ourTermAtIndex == 0) {
                // Nothing here yet — a plain new entry to append.
                log.append(entry);
            }
            // else: ourTermAtIndex == entry.term() already — we already
            // have this exact entry (e.g. a retried/duplicate RPC);
            // nothing to do, which makes handling the same AppendEntries
            // twice completely harmless.
        }

        // ===== Advance our commit index to match the leader's =====
        // We can never commit further than our OWN log actually
        // reaches yet, hence the Math.min.
        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), log.lastIndex());
            applyCommittedEntries();
        }

        return new AppendEntriesResponse(currentTerm, true);
    }

    /**
     * Common "we just learned about a newer term" handling, shared by
     * both RPC handlers above: adopt the new term, forget any vote we
     * cast in our old (now-stale) term, and revert to being a plain
     * follower no matter what role we held a moment ago.
     */
    private void stepDownToFollower(long newTerm) {
        currentTerm = newTerm;
        votedFor = null;
        role = Role.FOLLOWER;
    }

    // ==================== Starting and running an election ====================

    /**
     * Called by the timer-driving file when this node has gone too long
     * without hearing from a leader. Transitions to CANDIDATE, votes for
     * itself, and returns the RequestVoteRequest that should now be sent
     * to every peer (the actual sending happens outside this class, and
     * outside any lock, since network calls can be slow — see the class
     * javadoc above for why that split matters).
     */
    public synchronized RequestVoteRequest startElection() {
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = selfId;
        electionTerm = currentTerm;
        votesReceived = 1; // we always vote for ourselves
        resetElectionTimer();
        logger.info("Starting election for term {}", currentTerm);
        return new RequestVoteRequest(currentTerm, selfId, log.lastIndex(), log.lastTerm());
    }

    /**
     * Called once per vote response received back from a peer (or if a
     * peer simply never responds in time — the transport layer just
     * never calls this for that peer, which correctly means its vote
     * simply doesn't count toward our majority).
     */
    public synchronized void handleRequestVoteResponse(long requestTerm, RequestVoteResponse response) {
        if (response.term() > currentTerm) {
            stepDownToFollower(response.term());
            return;
        }
        // Ignore a response to an election we've since moved on from
        // (e.g. we already became leader, or started a NEWER election,
        // or stepped down) — a late/delayed network response arriving
        // after the fact must never be allowed to corrupt our current
        // state.
        if (role != Role.CANDIDATE || requestTerm != electionTerm || requestTerm != currentTerm) {
            return;
        }
        if (response.voteGranted()) {
            votesReceived++;
            int majority = (peerIds.size() + 1) / 2 + 1; // +1 for ourselves in the group's total size
            if (votesReceived >= majority) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = selfId;
        nextIndex = new HashMap<>();
        matchIndex = new HashMap<>();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, log.lastIndex() + 1);
            matchIndex.put(peerId, 0);
        }
        logger.info("Became LEADER for term {}", currentTerm);
    }

    // ==================== Leader: proposing new commands ====================

    /**
     * Called when a client sends a write command to whichever node
     * currently believes it's the leader. Appends a new LogEntry to our
     * own log immediately (this does NOT mean the command is committed
     * yet — only that it's now in our log, ready to be replicated).
     * Returns the new entry's log index, or -1 if this node isn't
     * currently the leader (the caller — later client-facing wiring —
     * is responsible for redirecting the client elsewhere in that case).
     */
    public synchronized int proposeCommand(List<String> command) {
        if (role != Role.LEADER) {
            return -1;
        }
        LogEntry entry = new LogEntry(currentTerm, command);
        return log.append(entry);
    }

    // ==================== Leader: replicating to a specific peer ====================

    /**
     * Builds the AppendEntries request this leader should currently send
     * to one specific peer, based on what we believe that peer's log
     * looks like (nextIndex.get(peerId)). Called repeatedly by the
     * timer-driving file — once per heartbeat interval for every peer,
     * whether or not there's anything new to send (an empty `entries`
     * list is a valid, normal heartbeat).
     */
    public synchronized AppendEntriesRequest buildAppendEntriesFor(String peerId) {
        int nextIdx = nextIndex.getOrDefault(peerId, log.lastIndex() + 1);
        int prevLogIndex = nextIdx - 1;
        long prevLogTerm = log.termAt(prevLogIndex);

        List<LogEntry> entriesToSend;
        if (nextIdx <= log.lastIndex()) {
            // subList would need 0-indexed positions into a copy of the
            // log; simplest correct approach at our current scale is to
            // just collect everything from nextIdx through the end.
            entriesToSend = new java.util.ArrayList<>();
            for (int i = nextIdx; i <= log.lastIndex(); i++) {
                entriesToSend.add(log.get(i));
            }
        } else {
            entriesToSend = List.of(); // fully caught up - plain heartbeat
        }

        return new AppendEntriesRequest(currentTerm, selfId, prevLogIndex, prevLogTerm, entriesToSend, commitIndex);
    }

    /**
     * Called once per AppendEntries response received back from a peer.
     * `sentPrevLogIndex` and `sentEntryCount` describe exactly what WE
     * sent that this response is answering (needed because, by the time
     * a response comes back, our own nextIndex/matchIndex bookkeeping
     * must be updated relative to what was ACTUALLY SENT, not whatever
     * our state happens to be right now — the network is asynchronous,
     * so several requests to the same peer could be in flight at once).
     */
    public synchronized void handleAppendEntriesResponse(String peerId, long requestTerm, int sentPrevLogIndex,
                                                          int sentEntryCount, AppendEntriesResponse response) {
        if (response.term() > currentTerm) {
            stepDownToFollower(response.term());
            return;
        }
        if (role != Role.LEADER || requestTerm != currentTerm) {
            return; // stale response for an election/term we've moved on from
        }

        if (response.success()) {
            int newMatchIndex = sentPrevLogIndex + sentEntryCount;
            matchIndex.put(peerId, newMatchIndex);
            nextIndex.put(peerId, newMatchIndex + 1);
            advanceCommitIndexIfPossible();
        } else {
            // Log mismatch - back off by one and we'll naturally retry
            // with an earlier prevLogIndex on the next heartbeat cycle,
            // walking backward until we find a point both logs agree on.
            int current = nextIndex.getOrDefault(peerId, 1);
            nextIndex.put(peerId, Math.max(1, current - 1));
        }
    }

    /**
     * ===== Raft's subtle commit-index safety rule (section 5.4.2 of the
     * Raft paper) =====
     * You might expect "an index is committed once a majority of nodes
     * have it" to be the whole rule — but that's NOT SAFE on its own.
     * Consider: a leader from an OLDER term replicated an entry to a
     * majority, then crashed before ever finding out it was safe to
     * commit. A NEW leader (different, later term) could, in principle,
     * later overwrite that entry if it never independently confirms it
     * with a majority under ITS OWN term. Raft's fix: a leader is only
     * allowed to advance commitIndex to cover an entry from a PREVIOUS
     * term by ALSO replicating at least one entry from its OWN current
     * term to a majority first — once that happens, all earlier entries
     * come along "for free," transitively, because the log-matching
     * property guarantees anyone holding that later entry also holds
     * every entry before it, identically.
     * In code, this means: only ever try to advance commitIndex to an
     * index N whose LOG ENTRY'S OWN TERM equals our CURRENT term — never
     * directly commit an entry from an earlier term on its own.
     */
    private void advanceCommitIndexIfPossible() {
        for (int candidateIndex = log.lastIndex(); candidateIndex > commitIndex; candidateIndex--) {
            if (log.termAt(candidateIndex) != currentTerm) {
                continue; // see the big comment above - never commit an older term's entry directly
            }
            int replicatedCount = 1; // the leader itself always has this entry
            for (String peerId : peerIds) {
                if (matchIndex.getOrDefault(peerId, 0) >= candidateIndex) {
                    replicatedCount++;
                }
            }
            int majority = (peerIds.size() + 1) / 2 + 1;
            if (replicatedCount >= majority) {
                commitIndex = candidateIndex;
                applyCommittedEntries();
                return; // candidateIndex is the HIGHEST such index by construction (we walked downward), so we're done
            }
        }
    }

    // ==================== Applying committed entries to the state machine ====================

    /**
     * Runs on EVERY node (leader and followers alike) whenever
     * commitIndex advances: applies every not-yet-applied entry, in
     * order, to this node's own DataStore via CommandProcessor — the
     * exact same "apply a command" entry point used everywhere else in
     * this project. Also logs each entry to this node's own WAL first
     * (if one was provided — see the constructor's javadoc above for
     * why THIS is the correct moment to do that, not any earlier), and
     * remembers the resulting CommandResult in appliedResults so a
     * client-facing caller (ClientHandler, waiting on THIS specific
     * index after calling proposeCommand) can retrieve it afterward.
     */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);

            if (wal != null) {
                try {
                    wal.append(entry.command());
                } catch (IOException e) {
                    // Mirrors ClientHandler's own Phase 2/3 reasoning: if we
                    // can't durably log it, we still choose to apply it here
                    // (unlike a fresh client write, this command is ALREADY
                    // committed — a majority of the group has it whether or
                    // not THIS node's own disk cooperates) but we log the
                    // failure loudly, since this node's own crash recovery
                    // would otherwise silently miss this entry later.
                    logger.error("Failed to write committed entry {} to local WAL: {}", lastApplied, e.getMessage());
                }
            }

            CommandProcessor.CommandResult result = commandProcessor.process(entry.command());
            appliedResults.put(lastApplied, result);
        }

        // Bound appliedResults' size: nothing should ever need to look back
        // further than a few entries (only an in-flight client wait would),
        // so we drop anything older than that to stop this map growing
        // forever over a long-running server's lifetime.
        appliedResults.keySet().removeIf(index -> index < lastApplied - 1000);
    }

    /**
     * Called by the client-facing networking layer after proposeCommand,
     * repeatedly, until this returns non-null (meaning this node has now
     * applied that index) or it gives up waiting. Synchronized because it
     * reads appliedResults/lastApplied, the same fields applyCommittedEntries
     * (also synchronized, since it's only ever called from within another
     * synchronized method here) mutates.
     */
    public synchronized CommandProcessor.CommandResult getAppliedResult(int index) {
        return appliedResults.get(index);
    }
}
