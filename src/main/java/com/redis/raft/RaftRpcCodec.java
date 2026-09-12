package com.redis.raft;

import com.redis.raft.RaftMessages.AppendEntriesRequest;
import com.redis.raft.RaftMessages.AppendEntriesResponse;
import com.redis.raft.RaftMessages.RequestVoteRequest;
import com.redis.raft.RaftMessages.RequestVoteResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * RaftRpcCodec — Phase 7's fifth file. Converts the 4 RaftMessages types
 * to and from a plain `List<String>` — the exact same shape RESPParser
 * already knows how to read from a socket and write back out (recall:
 * that's just a RESP multibulk array of bulk strings, and it's what
 * ReplicationManager, in Phase 6, already reused for encoding a Redis
 * COMMAND over the wire). By encoding Raft's own RPCs into that same
 * shape, the entire network layer we built in Phase 3 — RESPParser's
 * parseCommand()/writeReply() — works completely unchanged for Raft
 * traffic too, even though Raft has nothing to do with Redis commands.
 * No new wire format, no new networking code, no external serialization
 * library — just a different, purpose-built INTERPRETATION of the same
 * "flat list of strings" shape.
 *
 * ===== The flattening problem this file exists to solve =====
 * A RequestVote message is easy: term, candidateId, lastLogIndex,
 * lastLogTerm — 4 fixed fields, each one naturally a single string.
 * AppendEntries is trickier: it carries a whole LIST of LogEntry objects,
 * and each LogEntry ITSELF holds a variable-length list of command words.
 * A flat `List<String>` has no built-in way to show where one entry ends
 * and the next begins — so we invent a simple, explicit "count-prefixed"
 * convention: before each variable-length section, we put a plain integer
 * saying exactly how many items follow. Both sides (encoder here, decoder
 * here) agree on this convention, so decoding is just "read the count,
 * then read exactly that many items" — no guessing, no ambiguity, no
 * delimiter characters that could ever collide with real data (a lesson
 * this project already learned from RESP's own OWN length-prefixed
 * design for bulk strings, back in Phase 3).
 */
public class RaftRpcCodec {

    // Tag strings identifying which of the 4 message types a given
    // List<String> represents — always the very first element. A peer
    // receiving an RPC checks this ONE field first to know how to
    // interpret everything after it.
    public static final String REQUEST_VOTE = "REQUEST_VOTE";
    public static final String REQUEST_VOTE_RESPONSE = "REQUEST_VOTE_RESPONSE";
    public static final String APPEND_ENTRIES = "APPEND_ENTRIES";
    public static final String APPEND_ENTRIES_RESPONSE = "APPEND_ENTRIES_RESPONSE";

    // ==================== RequestVote ====================

    public static List<String> encode(RequestVoteRequest request) {
        return List.of(
                REQUEST_VOTE,
                String.valueOf(request.term()),
                request.candidateId(),
                String.valueOf(request.lastLogIndex()),
                String.valueOf(request.lastLogTerm())
        );
    }

    private static RequestVoteRequest decodeRequestVoteRequest(List<String> parts) {
        return new RequestVoteRequest(
                Long.parseLong(parts.get(1)),
                parts.get(2),
                Integer.parseInt(parts.get(3)),
                Long.parseLong(parts.get(4))
        );
    }

    public static List<String> encode(RequestVoteResponse response) {
        return List.of(
                REQUEST_VOTE_RESPONSE,
                String.valueOf(response.term()),
                response.voteGranted() ? "1" : "0"
        );
    }

    public static RequestVoteResponse decodeRequestVoteResponse(List<String> parts) {
        return new RequestVoteResponse(Long.parseLong(parts.get(1)), parts.get(2).equals("1"));
    }

    // ==================== AppendEntries ====================

    public static List<String> encode(AppendEntriesRequest request) {
        List<String> words = new ArrayList<>();
        words.add(APPEND_ENTRIES);
        words.add(String.valueOf(request.term()));
        words.add(request.leaderId());
        words.add(String.valueOf(request.prevLogIndex()));
        words.add(String.valueOf(request.prevLogTerm()));
        words.add(String.valueOf(request.leaderCommit()));

        // The count-prefixed section described in the class comment
        // above: how many entries follow, then each entry as
        // (term, wordCount, word1, word2, ..., wordN).
        words.add(String.valueOf(request.entries().size()));
        for (LogEntry entry : request.entries()) {
            words.add(String.valueOf(entry.term()));
            words.add(String.valueOf(entry.command().size()));
            words.addAll(entry.command());
        }
        return words;
    }

    private static AppendEntriesRequest decodeAppendEntriesRequest(List<String> parts) {
        long term = Long.parseLong(parts.get(1));
        String leaderId = parts.get(2);
        int prevLogIndex = Integer.parseInt(parts.get(3));
        long prevLogTerm = Long.parseLong(parts.get(4));
        int leaderCommit = Integer.parseInt(parts.get(5));
        int entryCount = Integer.parseInt(parts.get(6));

        List<LogEntry> entries = new ArrayList<>(entryCount);
        // `cursor` walks forward through `parts` by hand as we consume
        // each variable-length entry — exactly the same "read a count,
        // then consume that many items, advancing your position as you
        // go" pattern RESPParser itself uses for reading bulk strings.
        int cursor = 7;
        for (int i = 0; i < entryCount; i++) {
            long entryTerm = Long.parseLong(parts.get(cursor++));
            int wordCount = Integer.parseInt(parts.get(cursor++));
            List<String> command = new ArrayList<>(parts.subList(cursor, cursor + wordCount));
            cursor += wordCount;
            entries.add(new LogEntry(entryTerm, command));
        }

        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    public static List<String> encode(AppendEntriesResponse response) {
        return List.of(
                APPEND_ENTRIES_RESPONSE,
                String.valueOf(response.term()),
                response.success() ? "1" : "0"
        );
    }

    public static AppendEntriesResponse decodeAppendEntriesResponse(List<String> parts) {
        return new AppendEntriesResponse(Long.parseLong(parts.get(1)), parts.get(2).equals("1"));
    }

    // ==================== Generic incoming-request dispatch ====================

    /**
     * Used ONLY on the RECEIVING end of an incoming connection, where we
     * don't yet know (until we look at the tag) whether a peer is
     * sending us a RequestVoteRequest or an AppendEntriesRequest — unlike
     * a RESPONSE, which is always read by whichever code already knows
     * exactly what kind of request it just sent out, and so can call the
     * specific decodeXxxResponse method directly. Returns a plain
     * `Object` that the caller (RaftTransport, the next file) checks the
     * type of — Java doesn't have a cleaner built-in way to say "one of
     * these two specific types" without introducing a shared interface
     * purely for this one dispatch, which didn't seem worth it for just
     * two cases.
     */
    public static Object decodeIncomingRequest(List<String> parts) {
        String tag = parts.get(0);
        return switch (tag) {
            case REQUEST_VOTE -> decodeRequestVoteRequest(parts);
            case APPEND_ENTRIES -> decodeAppendEntriesRequest(parts);
            default -> throw new IllegalArgumentException("Unknown Raft RPC tag: " + tag);
        };
    }
}
