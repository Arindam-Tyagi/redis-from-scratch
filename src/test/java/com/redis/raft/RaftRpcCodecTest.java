package com.redis.raft;

import com.redis.raft.RaftMessages.AppendEntriesRequest;
import com.redis.raft.RaftMessages.AppendEntriesResponse;
import com.redis.raft.RaftMessages.RequestVoteRequest;
import com.redis.raft.RaftMessages.RequestVoteResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for RaftRpcCodec — for every message type, encoding then decoding
 * must always reproduce the exact original object ("round-tripping"). This
 * matters especially here because, unlike most of this project's other
 * codecs, this one uses a hand-rolled count-prefixed convention for
 * AppendEntries' variable-length parts — exactly the kind of manual
 * indexing/counting logic where an off-by-one bug could easily hide.
 */
class RaftRpcCodecTest {

    @Test
    void requestVoteRequestRoundTrips() {
        RequestVoteRequest original = new RequestVoteRequest(5, "node2", 10, 4);
        List<String> encoded = RaftRpcCodec.encode(original);
        Object decoded = RaftRpcCodec.decodeIncomingRequest(encoded);
        assertEquals(original, decoded);
    }

    @Test
    void requestVoteResponseRoundTrips() {
        RequestVoteResponse original = new RequestVoteResponse(7, true);
        List<String> encoded = RaftRpcCodec.encode(original);
        RequestVoteResponse decoded = RaftRpcCodec.decodeRequestVoteResponse(encoded);
        assertEquals(original, decoded);

        RequestVoteResponse originalFalse = new RequestVoteResponse(7, false);
        assertEquals(originalFalse, RaftRpcCodec.decodeRequestVoteResponse(RaftRpcCodec.encode(originalFalse)));
    }

    @Test
    void appendEntriesRequestRoundTripsWithNoEntries() {
        // The plain-heartbeat case: an empty entries list must survive
        // the round trip as an empty list, not null or a crash.
        AppendEntriesRequest original = new AppendEntriesRequest(3, "leader", 5, 2, List.of(), 5);
        List<String> encoded = RaftRpcCodec.encode(original);
        Object decoded = RaftRpcCodec.decodeIncomingRequest(encoded);
        assertEquals(original, decoded);
    }

    @Test
    void appendEntriesRequestRoundTripsWithMultipleVariableLengthEntries() {
        // Entries with DIFFERENT command lengths, deliberately - this is
        // exactly the scenario that would expose an off-by-one in the
        // hand-rolled cursor-walking decode logic (e.g. a 1-word PING-
        // like command followed by a longer multi-word command).
        LogEntry entryOne = new LogEntry(1, List.of("SET", "foo", "bar"));
        LogEntry entryTwo = new LogEntry(2, List.of("DEL", "foo"));
        LogEntry entryThree = new LogEntry(2, List.of("HSET", "user:1", "name", "Ajju", "role", "dev"));

        AppendEntriesRequest original = new AppendEntriesRequest(
                2, "leader", 4, 1, List.of(entryOne, entryTwo, entryThree), 3);

        List<String> encoded = RaftRpcCodec.encode(original);
        Object decoded = RaftRpcCodec.decodeIncomingRequest(encoded);

        assertEquals(original, decoded);
        // Belt-and-suspenders: also check the entries individually, so a
        // failure here points straight at "the entries didn't decode
        // right" rather than requiring digging into a big equals() diff.
        AppendEntriesRequest decodedRequest = (AppendEntriesRequest) decoded;
        assertEquals(3, decodedRequest.entries().size());
        assertEquals(List.of("HSET", "user:1", "name", "Ajju", "role", "dev"),
                decodedRequest.entries().get(2).command());
    }

    @Test
    void appendEntriesResponseRoundTrips() {
        AppendEntriesResponse original = new AppendEntriesResponse(9, true);
        assertEquals(original, RaftRpcCodec.decodeAppendEntriesResponse(RaftRpcCodec.encode(original)));

        AppendEntriesResponse originalFalse = new AppendEntriesResponse(9, false);
        assertEquals(originalFalse, RaftRpcCodec.decodeAppendEntriesResponse(RaftRpcCodec.encode(originalFalse)));
    }

    @Test
    void decodeIncomingRequestDispatchesToTheCorrectType() {
        RequestVoteRequest voteRequest = new RequestVoteRequest(1, "a", 0, 0);
        assertTrue(RaftRpcCodec.decodeIncomingRequest(RaftRpcCodec.encode(voteRequest)) instanceof RequestVoteRequest);

        AppendEntriesRequest appendRequest = new AppendEntriesRequest(1, "a", 0, 0, List.of(), 0);
        assertTrue(RaftRpcCodec.decodeIncomingRequest(RaftRpcCodec.encode(appendRequest)) instanceof AppendEntriesRequest);
    }

    @Test
    void decodeIncomingRequestRejectsAnUnknownTag() {
        assertThrows(IllegalArgumentException.class,
                () -> RaftRpcCodec.decodeIncomingRequest(List.of("SOMETHING_UNKNOWN", "1", "2")));
    }
}
