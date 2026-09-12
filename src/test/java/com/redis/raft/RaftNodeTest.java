package com.redis.raft;

import com.redis.core.CommandProcessor;
import com.redis.core.DataStore;
import com.redis.raft.RaftMessages.AppendEntriesRequest;
import com.redis.raft.RaftMessages.AppendEntriesResponse;
import com.redis.raft.RaftMessages.RequestVoteRequest;
import com.redis.raft.RaftMessages.RequestVoteResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for RaftNode — exercising the state machine directly, exactly the
 * way RaftNode.java was built to allow: no sockets, no threads, no timing,
 * just plain method calls simulating what a real network of 3 nodes would
 * exchange. Each test builds 2-3 RaftNode instances by hand (each with its
 * own RaftLog and DataStore/CommandProcessor, exactly like 3 separate real
 * processes would have) and manually feeds one node's output into
 * another's input, playing the role the (not-yet-built) network transport
 * layer will play for real later.
 */
class RaftNodeTest {

    private RaftNode newNode(String selfId, List<String> peerIds) {
        RaftLog log = new RaftLog();
        CommandProcessor commandProcessor = new CommandProcessor(new DataStore());
        return new RaftNode(selfId, peerIds, log, commandProcessor);
    }

    @Test
    void candidateBecomesLeaderAfterReceivingMajorityOfVotes() {
        RaftNode a = newNode("a", List.of("b", "c"));
        RaftNode b = newNode("b", List.of("a", "c"));
        RaftNode c = newNode("c", List.of("a", "b"));

        RequestVoteRequest request = a.startElection();
        assertEquals(RaftNode.Role.CANDIDATE, a.getRole());

        // Only need ONE of the two peers to grant a vote: 1 (self) + 1 = 2,
        // which is already a majority of 3.
        RequestVoteResponse responseFromB = b.handleRequestVote(request);
        assertTrue(responseFromB.voteGranted());

        a.handleRequestVoteResponse(request.term(), responseFromB);

        assertEquals(RaftNode.Role.LEADER, a.getRole());
        assertEquals("a", a.getLeaderId());
    }

    @Test
    void voteIsRejectedWhenCandidateTermIsStale() {
        RaftNode voter = newNode("voter", List.of("ghost"));

        // Force the voter's term up to 10 by having it process an
        // AppendEntries from a (simulated) leader already at term 10 -
        // the exact same "any higher term wins" rule every RPC obeys.
        AppendEntriesRequest bumpTerm = new AppendEntriesRequest(10, "ghost", 0, 0, List.of(), 0);
        voter.handleAppendEntries(bumpTerm);
        assertEquals(10, voter.getCurrentTerm());

        // A candidate still at term 1 asks for a vote - hopelessly stale.
        RequestVoteRequest staleRequest = new RequestVoteRequest(1, "latecomer", 0, 0);
        RequestVoteResponse response = voter.handleRequestVote(staleRequest);

        assertFalse(response.voteGranted());
        assertEquals(10, response.term()); // tells the stale candidate the real current term
    }

    @Test
    void voteIsRejectedWhenCandidateLogIsLessUpToDateThanVoters() {
        // Build the voter with a log that already has 2 entries at term 1
        // - a longer, more "up to date" log than the candidate is about
        // to claim it has.
        RaftLog voterLog = new RaftLog();
        voterLog.append(new LogEntry(1, List.of("SET", "a", "1")));
        voterLog.append(new LogEntry(1, List.of("SET", "b", "2")));
        RaftNode voter = new RaftNode("voter", List.of("candidate"), voterLog, new CommandProcessor(new DataStore()));

        // A candidate claiming the SAME term (1) but a SHORTER log
        // (lastLogIndex=1, i.e. only 1 entry) must be refused - the
        // voter's own log is strictly more up-to-date.
        RequestVoteRequest request = new RequestVoteRequest(1, "candidate", 1, 1);
        RequestVoteResponse response = voter.handleRequestVote(request);

        assertFalse(response.voteGranted());
    }

    @Test
    void conflictingFollowerEntryGetsTruncatedAndReplacedByLeadersVersion() {
        // The follower already has 2 entries, both from term 1 - but
        // its SECOND entry is about to conflict with what the (new,
        // legitimate) leader says should be there.
        RaftLog followerLog = new RaftLog();
        followerLog.append(new LogEntry(1, List.of("SET", "x", "old")));
        followerLog.append(new LogEntry(1, List.of("SET", "y", "old")));
        RaftNode follower = new RaftNode("follower", List.of("leader"), followerLog, new CommandProcessor(new DataStore()));

        // Leader (now at term 2) says: "your index 1 should be term 1
        // (matches - good), and here's a NEW entry for index 2, from MY
        // term 2" - this conflicts with the follower's existing term-1
        // entry at index 2.
        LogEntry leadersEntry = new LogEntry(2, List.of("SET", "y", "new"));
        AppendEntriesRequest request = new AppendEntriesRequest(2, "leader", 1, 1, List.of(leadersEntry), 0);

        AppendEntriesResponse response = follower.handleAppendEntries(request);

        assertTrue(response.success());
        assertEquals(2, followerLog.lastIndex()); // old conflicting entry replaced, not just appended after
        assertEquals(2, followerLog.termAt(2));
        assertEquals(List.of("SET", "y", "new"), followerLog.get(2).command());
    }

    @Test
    void appendEntriesFailsWhenPrevLogEntryDoesNotMatch() {
        RaftLog followerLog = new RaftLog();
        followerLog.append(new LogEntry(1, List.of("SET", "x", "1"))); // index 1, term 1

        RaftNode follower = new RaftNode("follower", List.of("leader"), followerLog, new CommandProcessor(new DataStore()));

        // Leader claims index 1 should be term 5 - but the follower's
        // actual index 1 is term 1. This must be rejected so the leader
        // knows to walk backward and find where they actually agree.
        AppendEntriesRequest request = new AppendEntriesRequest(2, "leader", 1, 5, List.of(), 0);
        AppendEntriesResponse response = follower.handleAppendEntries(request);

        assertFalse(response.success());
    }

    @Test
    void commitIndexOnlyAdvancesOnceMajorityReplicatesAnEntryFromLeadersOwnTerm() {
        RaftNode leader = newNode("leader", List.of("b", "c"));
        RaftNode nodeB = newNode("b", List.of("leader", "c"));
        RaftNode nodeC = newNode("c", List.of("leader", "b"));

        // Elect "leader" for real, via nodeB's vote.
        RequestVoteRequest voteRequest = leader.startElection();
        leader.handleRequestVoteResponse(voteRequest.term(), nodeB.handleRequestVote(voteRequest));
        assertEquals(RaftNode.Role.LEADER, leader.getRole());
        long leaderTerm = leader.getCurrentTerm();

        // Client write arrives at the leader - appended to ITS log
        // immediately, but nothing is committed yet.
        int index = leader.proposeCommand(List.of("SET", "foo", "bar"));
        assertEquals(1, index);

        // Replicate to nodeB only (nodeC hasn't received it yet - real
        // life would eventually retry it too, but this test is checking
        // the majority math specifically: 2 out of 3, including the
        // leader itself, is already enough).
        AppendEntriesRequest toB = leader.buildAppendEntriesFor("b");
        AppendEntriesResponse fromB = nodeB.handleAppendEntries(toB);
        assertTrue(fromB.success());

        leader.handleAppendEntriesResponse("b", leaderTerm, toB.prevLogIndex(), toB.entries().size(), fromB);

        // We don't have direct access to `leader`'s own CommandProcessor
        // from outside RaftNode (intentional encapsulation - RaftNode
        // owns applying committed entries itself). So we confirm the
        // commit happened INDIRECTLY instead: build the next heartbeat
        // this leader would send to nodeC and check its leaderCommit
        // field - that field is always set to the leader's OWN current
        // commitIndex (see buildAppendEntriesFor), so seeing it at 1
        // here proves the leader's commitIndex genuinely advanced to 1
        // after just nodeB's single acknowledgment - exactly the "2 out
        // of 3, including the leader itself" majority we expect.
        AppendEntriesRequest heartbeatToC = leader.buildAppendEntriesFor("c");
        assertEquals(1, heartbeatToC.leaderCommit());
    }
}
