package com.redis.raft;

import com.redis.core.CommandProcessor;
import com.redis.network.RESPParser;
import com.redis.raft.RaftMessages.AppendEntriesRequest;
import com.redis.raft.RaftMessages.AppendEntriesResponse;
import com.redis.raft.RaftMessages.RequestVoteRequest;
import com.redis.raft.RaftMessages.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * RaftTransport — Phase 7's sixth file, and the one that makes everything
 * else in this phase actually RUN. RaftNode (file 4) is pure logic with no
 * sense of time or network; this file supplies both: it opens a socket to
 * listen for incoming RPCs from peers, opens outbound sockets to SEND RPCs
 * to peers, and runs the timers that decide WHEN to start an election or
 * send a heartbeat. One RaftTransport exists per RaftNode (i.e. per group
 * membership) — a node participating in 3 different shards' Raft groups
 * (not something this project does, but worth naming) would need 3 of
 * each.
 *
 * ===== One timer thread, doing double duty =====
 * Real Raft describes election timeouts and heartbeats as if they were
 * two separate concerns, but they're really just two different answers to
 * the same question, asked repeatedly: "given my CURRENT role, what
 * should I be doing right now?" A LEADER should be sending heartbeats. A
 * FOLLOWER or CANDIDATE should be watching the clock for an election
 * timeout. We implement this as ONE recurring tick() (every
 * ELECTION_CHECK_INTERVAL_MILLIS, via ScheduledExecutorService — the
 * exact same tool ExpiryManager, Phase 4, used for its own background
 * sweep) that simply checks raftNode.getRole() and does whichever of
 * those two things is currently appropriate. Simpler than juggling two
 * independent schedules, and correctness only ever depends on the CURRENT
 * role, not on which timer happened to fire.
 *
 * ===== Why every RPC gets its own virtual thread =====
 * Sending a RequestVote/AppendEntries to a peer means real network I/O —
 * connecting a socket, writing bytes, waiting for a reply — which can be
 * slow, or can simply time out if that peer is down. If tick() sent RPCs
 * to peer 1, THEN waited for that to finish before contacting peer 2, one
 * dead peer would delay (or block) every OTHER peer's heartbeat too. So
 * exactly like Server.java (Phase 3) spins up one virtual thread per
 * client connection, we spin up one virtual thread per OUTBOUND RPC —
 * every peer gets contacted at essentially the same instant, completely
 * independently, and one unreachable peer's timeout has zero effect on
 * how quickly the others hear from us.
 *
 * ===== Randomized election timeouts, explained =====
 * If every follower used the EXACT SAME timeout, and a leader died, ALL
 * of them would become candidates at the exact same instant, split every
 * vote evenly among themselves, no one would reach a majority, and the
 * whole group would have to try again — potentially repeating forever in
 * the worst case. Randomizing each node's own timeout (independently, and
 * AGAIN every time it starts a new election) makes it overwhelmingly
 * likely that ONE node's timer fires meaningfully before the others',
 * giving it time to collect votes and become leader before anyone else
 * even starts a competing election.
 */
public class RaftTransport {

    private static final Logger logger = LoggerFactory.getLogger(RaftTransport.class);

    // How often the single timer thread wakes up to re-check "what should
    // I be doing right now?" Small relative to the election timeout, so
    // an actual timeout is detected promptly rather than with a long lag.
    private static final long ELECTION_CHECK_INTERVAL_MILLIS = 50;

    // Randomized election timeout range: every node picks a fresh random
    // value in [300, 600) milliseconds every time it starts a new
    // election. Comfortably larger than HEARTBEAT_INTERVAL_MILLIS below,
    // so a handful of missed/slow heartbeats don't trigger a needless
    // election under normal conditions.
    private static final long ELECTION_TIMEOUT_MIN_MILLIS = 300;
    private static final long ELECTION_TIMEOUT_RANGE_MILLIS = 300;

    // How often a LEADER sends AppendEntries (real entries if there's
    // anything new, otherwise an empty heartbeat) to every peer.
    private static final long HEARTBEAT_INTERVAL_MILLIS = 100;

    // How long any single outbound RPC will wait — for the initial socket
    // connection AND for a reply — before giving up on that one peer for
    // this round. A down/unreachable peer should never be allowed to hang
    // this node indefinitely.
    private static final int RPC_TIMEOUT_MILLIS = 200;

    /** One peer's Raft-RPC network address — deliberately separate from
     * that node's normal Redis client-facing port (see the config file
     * changes later in this phase: every node gets a SECOND port,
     * dedicated purely to Raft peer-to-peer traffic). */
    public record PeerAddress(String host, int port) {
    }

    private final String selfId;
    private final int raftPort;
    private final RaftNode raftNode;
    private final Map<String, PeerAddress> peers; // peerId -> where to reach them

    private final ScheduledExecutorService scheduler;
    private final Random random = new Random();
    private volatile long electionTimeoutMillis;

    public RaftTransport(String selfId, int raftPort, RaftNode raftNode, Map<String, PeerAddress> peers) {
        this.selfId = selfId;
        this.raftPort = raftPort;
        this.raftNode = raftNode;
        this.peers = peers;
        this.electionTimeoutMillis = randomElectionTimeout();

        ThreadFactory daemonThreadFactory = runnable -> {
            Thread thread = new Thread(runnable, "raft-timer-" + selfId);
            thread.setDaemon(true);
            return thread;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory);
    }

    private long randomElectionTimeout() {
        return ELECTION_TIMEOUT_MIN_MILLIS + random.nextLong(ELECTION_TIMEOUT_RANGE_MILLIS);
    }

    /** Starts both halves of this class: the incoming-RPC listener (its
     * own dedicated thread, since it blocks forever accepting
     * connections) and the recurring timer tick. */
    public void start() {
        Thread acceptorThread = new Thread(this::runIncomingServer, "raft-acceptor-" + selfId);
        acceptorThread.setDaemon(true);
        acceptorThread.start();

        scheduler.scheduleWithFixedDelay(
                this::tick, ELECTION_CHECK_INTERVAL_MILLIS, ELECTION_CHECK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

        logger.info("Raft transport started for '{}' on port {} ({} peer(s))", selfId, raftPort, peers.size());
    }

    public void stop() {
        scheduler.shutdown();
    }

    // ==================== Incoming RPCs ====================

    private void runIncomingServer() {
        try (ServerSocket serverSocket = new ServerSocket(raftPort)) {
            while (true) {
                Socket socket = serverSocket.accept();
                // One virtual thread per incoming RPC connection — same
                // reasoning as Server.java's client connections, just a
                // much shorter-lived conversation (exactly one
                // request/response, then done).
                Thread.ofVirtual().start(() -> handleIncomingConnection(socket));
            }
        } catch (IOException e) {
            logger.error("Raft transport listener on port {} failed", raftPort, e);
        }
    }

    private void handleIncomingConnection(Socket socket) {
        try (Socket s = socket; InputStream in = s.getInputStream(); OutputStream out = s.getOutputStream()) {
            RESPParser parser = new RESPParser(in);
            List<String> requestWords = parser.parseCommand();
            if (requestWords == null || requestWords.isEmpty()) {
                return;
            }

            Object request = RaftRpcCodec.decodeIncomingRequest(requestWords);
            List<String> responseWords;
            if (request instanceof RequestVoteRequest voteRequest) {
                RequestVoteResponse response = raftNode.handleRequestVote(voteRequest);
                responseWords = RaftRpcCodec.encode(response);
            } else if (request instanceof AppendEntriesRequest appendRequest) {
                AppendEntriesResponse response = raftNode.handleAppendEntries(appendRequest);
                responseWords = RaftRpcCodec.encode(response);
            } else {
                return; // decodeIncomingRequest only ever returns one of the two types above
            }

            writeWords(out, responseWords);
        } catch (IOException e) {
            // A peer's connection dropping mid-RPC is a normal, expected
            // network occurrence (exactly like ClientHandler's own
            // reasoning in Phase 3) — log quietly and move on; it does
            // not affect any other in-flight RPC or this node's own state.
            logger.debug("Raft RPC connection error: {}", e.getMessage());
        }
    }

    // ==================== The one recurring timer tick ====================

    private void tick() {
        // Wrapped in try/catch for the exact same reason ExpiryManager's
        // sweep is (Phase 4): an uncaught exception inside a
        // ScheduledExecutorService task silently cancels ALL future runs
        // of it — which here would mean this node quietly stops
        // participating in elections/heartbeats forever, with no obvious
        // error printed anywhere. Never acceptable for something this
        // central to the node's correctness.
        try {
            if (raftNode.getRole() == RaftNode.Role.LEADER) {
                sendHeartbeatsToAllPeers();
            } else if (System.currentTimeMillis() - raftNode.getLastResetAt() >= electionTimeoutMillis) {
                startElectionAndRequestVotes();
            }
        } catch (Exception e) {
            logger.error("Unexpected error in Raft timer tick for '{}'", selfId, e);
        }
    }

    private void startElectionAndRequestVotes() {
        electionTimeoutMillis = randomElectionTimeout(); // fresh random value for the NEXT round
        RequestVoteRequest request = raftNode.startElection();

        for (String peerId : peers.keySet()) {
            Thread.ofVirtual().start(() -> {
                try {
                    RequestVoteResponse response = sendRequestVote(peerId, request);
                    raftNode.handleRequestVoteResponse(request.term(), response);
                } catch (IOException e) {
                    logger.debug("RequestVote to '{}' failed: {}", peerId, e.getMessage());
                }
            });
        }
    }

    private void sendHeartbeatsToAllPeers() {
        for (String peerId : peers.keySet()) {
            // Built freshly for EACH peer (their nextIndex can legitimately
            // differ from each other) and captured into plain local
            // variables here, BEFORE handing off to a separate thread —
            // this snapshot is exactly "what we actually sent," which is
            // what handleAppendEntriesResponse needs to correctly update
            // nextIndex/matchIndex later, regardless of whatever this
            // node's OWN state has moved on to by the time the reply
            // eventually arrives.
            AppendEntriesRequest request = raftNode.buildAppendEntriesFor(peerId);
            long sentTerm = request.term();
            int sentPrevLogIndex = request.prevLogIndex();
            int sentEntryCount = request.entries().size();

            Thread.ofVirtual().start(() -> {
                try {
                    AppendEntriesResponse response = sendAppendEntries(peerId, request);
                    raftNode.handleAppendEntriesResponse(peerId, sentTerm, sentPrevLogIndex, sentEntryCount, response);
                } catch (IOException e) {
                    logger.debug("AppendEntries to '{}' failed: {}", peerId, e.getMessage());
                }
            });
        }
    }

    // ==================== Outbound RPCs ====================

    private RequestVoteResponse sendRequestVote(String peerId, RequestVoteRequest request) throws IOException {
        List<String> reply = sendAndReceive(peerId, RaftRpcCodec.encode(request));
        return RaftRpcCodec.decodeRequestVoteResponse(reply);
    }

    private AppendEntriesResponse sendAppendEntries(String peerId, AppendEntriesRequest request) throws IOException {
        List<String> reply = sendAndReceive(peerId, RaftRpcCodec.encode(request));
        return RaftRpcCodec.decodeAppendEntriesResponse(reply);
    }

    /**
     * Opens a brand-new, short-lived socket for exactly one RPC — connect,
     * send, receive, close. We don't bother keeping persistent connections
     * open between nodes (unlike Phase 6's replication stream, which HAS
     * to stay open indefinitely to carry a continuous flow of future
     * writes) — Raft's RPCs are small, frequent, and independent of each
     * other, so a fresh connection per call is simpler to reason about
     * and avoids ever having to detect/recover a "stale but still open"
     * connection.
     */
    private List<String> sendAndReceive(String peerId, List<String> encodedRequest) throws IOException {
        PeerAddress address = peers.get(peerId);
        if (address == null) {
            throw new IOException("No known address for peer '" + peerId + "'");
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(address.host(), address.port()), RPC_TIMEOUT_MILLIS);
            socket.setSoTimeout(RPC_TIMEOUT_MILLIS);

            writeWords(socket.getOutputStream(), encodedRequest);

            RESPParser parser = new RESPParser(socket.getInputStream());
            List<String> reply = parser.parseCommand();
            if (reply == null) {
                throw new IOException("Peer '" + peerId + "' closed the connection without replying");
            }
            return reply;
        }
    }

    /**
     * Encodes a flat List<String> as a RESP array and writes it out —
     * the same "wrap each word as a bulk string inside an ARRAY
     * CommandResult" trick ReplicationManager (Phase 6) used to send a
     * command over the wire, reused here for Raft's own RPCs.
     */
    private static void writeWords(OutputStream out, List<String> words) throws IOException {
        List<CommandProcessor.CommandResult> items = new ArrayList<>(words.size());
        for (String word : words) {
            items.add(CommandProcessor.CommandResult.bulkString(word));
        }
        RESPParser.writeReply(out, CommandProcessor.CommandResult.array(items));
    }
}
