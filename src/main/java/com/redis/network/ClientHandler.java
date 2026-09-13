package com.redis.network;

import com.redis.core.CommandProcessor;
import com.redis.core.CommandProcessor.CommandResult;
import com.redis.core.DataStore;
import com.redis.persistence.WriteAheadLog;
import com.redis.raft.RaftNode;
import com.redis.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

/**
 * ClientHandler — Phase 3, file 2. Represents ONE connected client and the
 * whole conversation with it: read a command, log+apply it, reply, repeat,
 * until the client disconnects or something goes wrong.
 *
 * ===== Socket — the first networking concept in this project =====
 * A `Socket` is Java's representation of one live, already-established TCP
 * connection between our server and one client. Think of a socket as a
 * two-way pipe: bytes the client sends arrive on `socket.getInputStream()`,
 * and bytes we write to `socket.getOutputStream()` get sent back to that
 * client. Once we have a Socket, reading/writing it looks EXACTLY like
 * reading/writing any other InputStream/OutputStream — which is precisely
 * why RESPParser, built purely against those two generic interfaces back
 * in the previous file, needs zero changes to work with a real network
 * connection: we just hand it the socket's own streams.
 *
 * A Socket is created for us — we, as the SERVER, don't "connect" anywhere;
 * we accept an incoming connection a client already initiated. That
 * accepting happens in Server.java (the next file), which will hand us the
 * resulting Socket already fully connected and ready to read/write.
 *
 * ===== implements Runnable =====
 * Runnable is one of Java's oldest and simplest interfaces: it declares
 * exactly one method, `void run()`, with no arguments and no return value.
 * It represents "a unit of work that can be handed to some thread to
 * execute." We make ClientHandler implement Runnable specifically so
 * Server.java can do `Thread.ofVirtual().start(clientHandler)` — handing
 * this whole object to a brand-new virtual thread, which will call our
 * run() method and, from that point on, THIS specific client's entire
 * conversation happens on THAT one thread, completely independently of
 * every other connected client's own thread. This is what makes "handle
 * many clients at once" possible at all: each ClientHandler instance/thread
 * pair only ever thinks about ONE client, one command at a time, in a
 * simple sequential loop — no manual juggling of multiple clients within
 * one thread.
 */
public class ClientHandler implements Runnable {

    // A Logger, not System.out.println — this is SLF4J, the logging
    // dependency from pom.xml, finally put to use. LoggerFactory.getLogger
    // takes a Class so that every log line automatically records WHICH
    // class produced it — hugely useful once we have a server juggling
    // many simultaneous client threads and, eventually, multiple
    // replicating nodes all logging at once.
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket clientSocket;
    private final CommandProcessor commandProcessor;
    private final WriteAheadLog wal;

    // ===== Phase 6 additions =====
    // dataStore: needed to build a full-sync dump when a replica connects
    // and sends SYNC (see handleSync() below).
    // replicationManager: null on a REPLICA node's own Server (replicas
    // don't accept connections from further sub-replicas in this
    // project), non-null on a PRIMARY node's Server. Exactly the same
    // "null means this feature is off" pattern CommandProcessor already
    // uses for its own ClusterConfig field.
    private final DataStore dataStore;
    private final ReplicationManager replicationManager;

    // ===== Phase 7 addition =====
    // Non-null exactly when this node participates in a Raft group. When
    // present, it takes over WRITE commands entirely (see
    // handleOneCommand below) - a write no longer goes straight to
    // CommandProcessor/WAL the Phase 1-6 way; it goes through Raft
    // consensus first, and only gets applied (and logged to the WAL) once
    // a majority agrees, from inside RaftNode.applyCommittedEntries. Read
    // commands are completely unaffected either way - they just read
    // this node's own local DataStore directly, same as always.
    private final RaftNode raftNode;

    public ClientHandler(Socket clientSocket, CommandProcessor commandProcessor, WriteAheadLog wal,
                          DataStore dataStore, ReplicationManager replicationManager, RaftNode raftNode) {
        this.clientSocket = clientSocket;
        this.commandProcessor = commandProcessor;
        this.wal = wal;
        this.dataStore = dataStore;
        this.replicationManager = replicationManager;
        this.raftNode = raftNode;
    }

    /**
     * The method a virtual thread calls once, when started, and which runs
     * for as long as this client stays connected. When this method
     * RETURNS, that thread's job is done and the thread itself terminates
     * (virtual threads are cheap enough that we don't reuse/pool them —
     * each connection simply gets its own, and it's fine for that thread
     * to end when the connection ends).
     */
    @Override
    public void run() {
        // try-with-resources on the Socket itself: when this try block
        // exits — normally OR via an exception — Socket.close() is called
        // automatically, which also closes the InputStream/OutputStream
        // obtained from it. This guarantees we never "leak" an open socket
        // just because a client's connection had a problem partway through.
        try (Socket socket = clientSocket;
             InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {

            RESPParser parser = new RESPParser(in);
            logger.info("Client connected: {}", socket.getRemoteSocketAddress());

            while (true) {
                List<String> args;
                try {
                    args = parser.parseCommand();
                } catch (RESPParser.ProtocolException e) {
                    // The client sent something we couldn't understand as
                    // RESP or an inline command. We tell them exactly that,
                    // then close the connection — there's no reasonable way
                    // to "resynchronize" with a client that isn't speaking
                    // the protocol correctly, so continuing to read more
                    // bytes from a confused stream would likely just cause
                    // more confusion.
                    logger.warn("Protocol error from {}: {}", socket.getRemoteSocketAddress(), e.getMessage());
                    RESPParser.writeReply(out, CommandResult.error("ERR Protocol error: " + e.getMessage()));
                    return;
                }

                if (args == null) {
                    // parseCommand() returning null means the client closed
                    // the connection cleanly — nothing more to do here.
                    logger.info("Client disconnected: {}", socket.getRemoteSocketAddress());
                    return;
                }

                if (args.isEmpty()) {
                    // An empty inline command (e.g. the client just sent a
                    // blank line) — real Redis silently ignores these
                    // rather than treating them as an error, so we do too.
                    continue;
                }

                // ===== Phase 6: a replica introducing itself =====
                // SYNC is special: it's not a normal CommandProcessor
                // command at all (it never touches DataStore), and it gets
                // NO normal reply — handleSync() writes the full dataset
                // dump directly, then registers this connection to keep
                // receiving future writes. We deliberately check this
                // BEFORE calling handleOneCommand()/writeReply() below, so
                // SYNC never falls through to CommandProcessor (which
                // would otherwise treat it as an unknown command and
                // reply with an error).
                if (replicationManager != null && args.get(0).equalsIgnoreCase("SYNC")) {
                    handleSync(out);
                    // Loop straight back to parser.parseCommand(). A
                    // replica never sends anything else after SYNC, so
                    // that call will simply block, parking this thread
                    // harmlessly, for as long as this replica stays
                    // connected — exactly what we want: the socket (and
                    // its OutputStream, now registered with
                    // replicationManager) stays open and ready to receive
                    // future propagated writes, without this thread
                    // spinning or doing anything else in the meantime.
                    continue;
                }

                CommandResult result = handleOneCommand(args);
                RESPParser.writeReply(out, result);

                // ===== Phase 6: propagate successful writes to replicas =====
                // Runs AFTER the reply is sent to the real client, and
                // only for commands that (a) are write commands and (b)
                // actually succeeded (didn't come back as an ERROR, e.g.
                // from a WRONGTYPE or wrong-argument-count problem) — a
                // failed write never mutated DataStore, so there is
                // nothing for a replica to apply either.
                if (replicationManager != null
                        && CommandProcessor.isWriteCommand(args.get(0))
                        && result.getType() != CommandResult.Type.ERROR) {
                    replicationManager.propagate(args);
                }
            }
        } catch (IOException e) {
            // A real network problem — the client's connection dropped
            // unexpectedly, a timeout, etc. This is a NORMAL, expected
            // occurrence for a network server (clients disconnect
            // ungracefully all the time) rather than a bug in our code, so
            // we log it at a low severity and simply let this thread end;
            // it does NOT crash the server or affect any other connected
            // client in any way, since each client's entire handling lives
            // on its own independent thread.
            logger.info("Connection closed for {}: {}", clientSocket.getRemoteSocketAddress(), e.getMessage());
        }
    }

    /**
     * Runs ONE command: logs it to the WAL first if (and only if) it's a
     * write command, THEN applies it via CommandProcessor.
     *
     * ===== WHY THE WAL WRITE MUST HAPPEN BEFORE process() IS CALLED =====
     * This is the actual "write-AHEAD" part of Write-Ahead Logging, made
     * concrete: if we applied the command to DataStore first and only
     * logged it afterward, a crash in between those two steps would leave
     * DataStore holding a change that was NEVER made durable — after
     * restart, replaying the WAL would never reproduce it, and that data
     * is gone forever, even though (from the client's perspective) the
     * command may have already succeeded. By logging FIRST, the worst case
     * of a crash between these two lines is: the command is durably
     * recorded but not yet applied to THIS run's in-memory DataStore — and
     * replaying the WAL on restart fixes exactly that, applying it then.
     * The order of these two lines is not a style choice; it's the entire
     * correctness guarantee of this whole subsystem.
     *
     * Only commands CommandProcessor.isWriteCommand(...) confirms are
     * mutating get logged at all — logging every GET/PING would bloat the
     * WAL with entries that would do nothing useful on replay.
     *
     * We log even a command that MIGHT turn out to be invalid (wrong
     * argument count, wrong type, etc.) — process() runs after the log
     * write regardless, and if it returns an ERROR CommandResult, nothing
     * in DataStore was ever actually mutated for that command (every
     * command method in CommandProcessor validates BEFORE mutating). So
     * replaying an invalid, previously-logged command later is completely
     * harmless: it will fail validation again, identically, and mutate
     * nothing then either — the log just carries a few harmless wasted
     * bytes for that one bad command, which is an entirely acceptable
     * trade for keeping this logic simple.
     */
    private CommandResult handleOneCommand(List<String> args) {
        String commandName = args.get(0);

        // ===== Phase 7: writes on a Raft-managed node go through consensus =====
        // raftNode is only non-null for a node running Raft (every one of
        // our 9 Phase 7 nodes). For those, a write command bypasses the
        // Phase 1-6 "WAL then apply directly" path entirely - see
        // handleRaftWrite's own comment for the full reasoning. Reads
        // (GET, LRANGE, etc.) and PING fall straight through to the
        // ordinary path below unchanged, since they never need
        // consensus - they just answer from this node's own local,
        // already-consistent DataStore.
        if (raftNode != null && CommandProcessor.isWriteCommand(commandName)) {
            return handleRaftWrite(args);
        }

        if (CommandProcessor.isWriteCommand(commandName)) {
            try {
                wal.append(args);
            } catch (IOException e) {
                // If we can't even durably LOG a write command, we must
                // refuse to apply it — silently applying a write we
                // couldn't make crash-durable would be a correctness bug
                // (the client would think it succeeded, but a crash right
                // after would lose it with no record it ever happened).
                // Failing loudly here, with a clear error back to the
                // client, is the honest thing to do.
                logger.error("Failed to write to WAL for command {}: {}", commandName, e.getMessage());
                return CommandResult.error("ERR failed to persist command: " + e.getMessage());
            }
        }

        return commandProcessor.process(args);
    }

    // How long (in milliseconds) we're willing to sit here waiting for a
    // proposed write to actually get committed and applied before giving
    // up and telling the client something went wrong, rather than
    // blocking this connection's thread forever. Comfortably longer than
    // a normal commit should ever take (a handful of heartbeat intervals,
    // ~100ms each) even accounting for an election happening mid-write.
    private static final long RAFT_WRITE_TIMEOUT_MILLIS = 2000;
    private static final long RAFT_WRITE_POLL_INTERVAL_MILLIS = 5;

    /**
     * Routes ONE write command through Raft consensus instead of applying
     * it directly, and waits for the result.
     *
     * ===== Why this blocks the calling thread, and why that's fine here =====
     * A real client sending SET expects a reply that reflects reality -
     * "OK" should mean the write is actually safe (replicated to a
     * majority), not just "a leader wrote it to ITS OWN memory and might
     * lose it if it crashes a millisecond later." So we can't reply the
     * instant proposeCommand() returns; we have to wait until this
     * specific index is committed AND applied. This method blocks the
     * CURRENT thread doing exactly that (a simple sleep-and-poll loop -
     * simpler to reason about than wiring up callbacks/futures for a
     * project at this scope, and perfectly fine here specifically BECAUSE
     * this is a virtual thread (Server.java) - blocking it costs nothing
     * beyond this one client's own reply latency, and every OTHER
     * client's virtual thread keeps running completely unaffected in the
     * meantime.
     */
    private CommandResult handleRaftWrite(List<String> args) {
        if (!raftNode.isLeader()) {
            String leaderHint = raftNode.getLeaderId();
            return CommandResult.error("TRYAGAIN not the leader for this shard"
                    + (leaderHint != null ? " - current leader is '" + leaderHint + "'" : " - leader unknown right now"));
        }

        int index = raftNode.proposeCommand(args);
        if (index == -1) {
            // Lost leadership in the tiny window between the isLeader()
            // check above and this call (e.g. a higher-term AppendEntries
            // arrived from a legitimate new leader) - tell the client to
            // simply retry, exactly like the branch above.
            return CommandResult.error("TRYAGAIN not the leader for this shard - please retry");
        }

        long deadline = System.currentTimeMillis() + RAFT_WRITE_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            CommandResult result = raftNode.getAppliedResult(index);
            if (result != null) {
                return result;
            }
            try {
                Thread.sleep(RAFT_WRITE_POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                // Restore the interrupt flag rather than swallowing it
                // silently — the standard, correct way to handle
                // InterruptedException when we're not actually going to
                // rethrow it (see ReplicaClient's sleepBeforeRetry in
                // Phase 6 for the same pattern, explained more fully).
                Thread.currentThread().interrupt();
                return CommandResult.error("ERR interrupted while waiting for write to commit");
            }
        }

        // Timed out - this can genuinely happen (e.g. an election was in
        // progress, or too many peers are unreachable to form a
        // majority). The command MAY still commit later; we simply
        // couldn't confirm that in time to answer this client, so we're
        // honest about the uncertainty rather than guessing OK or ERROR.
        return CommandResult.error("ERR timed out waiting for write to commit - it may or may not have succeeded");
    }

    /**
     * Handles an incoming SYNC command from a connecting replica: delegates
     * the actual work to ReplicationManager (which owns every piece of
     * replication-specific logic — see ReplicationManager.performFullSync),
     * passing it this connection's OutputStream and this node's DataStore.
     * If writing the full sync fails partway through (the replica
     * disconnected before we finished, say), we let the IOException
     * propagate up to run()'s own catch block, which already knows how to
     * log a dropped connection and clean up — no special handling needed
     * here beyond that.
     */
    private void handleSync(OutputStream out) throws IOException {
        logger.info("Replica connected from {} - sending full sync", clientSocket.getRemoteSocketAddress());
        replicationManager.performFullSync(out, dataStore);
    }
}
