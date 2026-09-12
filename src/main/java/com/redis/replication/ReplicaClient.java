package com.redis.replication;

import com.redis.core.CommandProcessor;
import com.redis.network.RESPParser;
import com.redis.persistence.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

/**
 * ReplicaClient — Phase 6's second file. This is the mirror image of
 * ReplicationManager: it runs on a REPLICA node, and its whole job is to
 * connect OUT to that replica's primary (as an ordinary client would),
 * receive the full sync, then keep applying whatever the primary streams
 * afterward — forever, for as long as this server process runs.
 *
 * `implements Runnable` — same pattern ClientHandler used in Phase 3 — so
 * Main.java (wired up later in this phase) can hand this to its own
 * dedicated background thread, one per replica node, completely separate
 * from that node's own Server (a replica ALSO still runs a normal
 * Server/ClientHandler setup, in case anyone connects to IT directly —
 * this class is purely the additional "pull updates from my primary"
 * responsibility layered on top).
 */
public class ReplicaClient implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaClient.class);

    // If the connection to the primary is ever lost (primary not started
    // yet, restarted, a network blip), how long to wait before trying
    // again — a real system would typically use "exponential backoff"
    // (doubling the wait each failed attempt, up to some cap) to avoid
    // hammering a primary that's still recovering; a fixed delay is a
    // reasonable simplification at this project's scale.
    private static final long RECONNECT_DELAY_MILLIS = 2000;

    private final String masterHost;
    private final int masterPort;
    private final CommandProcessor commandProcessor;
    private final WriteAheadLog wal;

    public ReplicaClient(String masterHost, int masterPort,
                          CommandProcessor commandProcessor, WriteAheadLog wal) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.commandProcessor = commandProcessor;
        this.wal = wal;
    }

    /**
     * Runs FOREVER on its own dedicated thread. Deliberately structured as
     * an outer "keep trying to (re)connect" loop wrapped around an inner
     * "stay connected and apply commands" loop — any failure in the inner
     * loop (the primary restarting, a dropped connection, etc.) throws an
     * IOException that unwinds back out here, gets logged, and after a
     * short pause we simply try connecting — and re-syncing completely
     * from scratch — all over again. A replica that gave up permanently
     * after one dropped connection would silently and permanently fall
     * out of sync, which is far worse than briefly retrying.
     */
    @Override
    public void run() {
        while (true) {
            try {
                connectAndReplicateForever();
            } catch (IOException e) {
                logger.warn("Lost connection to primary {}:{} - retrying in {}ms",
                        masterHost, masterPort, RECONNECT_DELAY_MILLIS, e);
            }
            sleepBeforeRetry();
        }
    }

    /**
     * `try (Socket socket = ...)` — Java's "try-with-resources" syntax.
     * Socket implements Closeable, and this syntax guarantees
     * socket.close() runs automatically once this block exits, WHETHER IT
     * exits normally or because an exception was thrown partway through —
     * exactly the same "always clean up" guarantee a manual
     * try/finally would give us, just with less boilerplate. This matters
     * here specifically because the loop below can exit via an
     * IOException at any point (a dropped connection), and we still want
     * the socket properly closed before this method's caller (run()'s
     * outer loop) tries reconnecting.
     */
    private void connectAndReplicateForever() throws IOException {
        logger.info("Connecting to primary at {}:{}...", masterHost, masterPort);
        try (Socket socket = new Socket(masterHost, masterPort)) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            sendSyncCommand(out);
            logger.info("Connected - receiving full sync and live updates from primary");

            // We reuse RESPParser here EXACTLY as ClientHandler does for
            // reading commands from a normal client — a command arriving
            // from our primary (whether it's part of the initial full
            // sync, or a live write forwarded later) is byte-for-byte the
            // same shape as a command a real client would send us, so the
            // exact same, already-tested parsing code applies unchanged.
            RESPParser parser = new RESPParser(in);
            while (true) {
                List<String> command = parser.parseCommand();
                if (command == null) {
                    // The primary closed the connection. We treat this
                    // exactly like any other failure: throw, let run()'s
                    // outer loop catch it, wait, and reconnect + re-sync
                    // from scratch.
                    throw new IOException("Primary closed the replication connection");
                }
                if (command.isEmpty()) {
                    continue; // shouldn't normally happen, but harmless to skip
                }
                applyReplicatedCommand(command);
            }
        }
    }

    /**
     * Sends the one and only command this replica ever SENDS to its
     * primary. From this point on, this connection carries data in ONE
     * direction only: primary -> replica. We build the SYNC command the
     * same way ReplicationManager encodes an outgoing write on the
     * primary side — wrapping it as an ARRAY of BULK_STRING
     * CommandResults and handing that to RESPParser.writeReply, since a
     * Redis command and a RESP array reply are, byte-for-byte, the exact
     * same shape.
     */
    private void sendSyncCommand(OutputStream out) throws IOException {
        CommandProcessor.CommandResult syncCommand = CommandProcessor.CommandResult.array(
                List.of(CommandProcessor.CommandResult.bulkString("SYNC")));
        RESPParser.writeReply(out, syncCommand);
    }

    /**
     * Applies one command received from the primary — treating a
     * "catch-up" command from the initial full sync and a "live" command
     * forwarded later completely identically; there's no meaningful
     * difference to this method, both are just "a command to apply now."
     *
     * We log it to THIS node's own WriteAheadLog first, then apply it —
     * the exact same log-then-apply ordering ClientHandler uses for a
     * normal client's write (see ClientHandler.java). This gives a
     * replica the SAME crash-durability guarantee a primary has: if this
     * replica process itself crashes and restarts, it recovers from its
     * own snapshot + WAL first, then reconnects to its primary and
     * re-syncs anything it might still be missing.
     */
    private void applyReplicatedCommand(List<String> command) throws IOException {
        String commandName = command.get(0);
        if (CommandProcessor.isWriteCommand(commandName)) {
            wal.append(command);
        }
        commandProcessor.process(command);
        // No reply is written back here — unlike ClientHandler, which
        // always replies to a real client, the primary isn't waiting to
        // hear anything back over this connection.
    }

    /**
     * Thread.sleep(millis) pauses the CURRENT thread (here, this
     * replica's dedicated background thread) for roughly that long.
     * It's a `checked` exception (InterruptedException) because another
     * thread could, in principle, call Thread.interrupt() on this thread
     * while it's sleeping, to ask it to wake up early and stop what it's
     * doing. We don't currently have any code that does that to this
     * thread, but the JDK forces us to at least acknowledge the
     * possibility. The standard, correct way to handle an
     * InterruptedException you're not going to act on immediately
     * yourself is to call Thread.currentThread().interrupt() — this
     * "re-raises" the interrupt flag on the current thread rather than
     * silently swallowing it, so that ANY other code further up the call
     * stack that might care about this thread being interrupted can still
     * find out about it later.
     */
    private void sleepBeforeRetry() {
        try {
            Thread.sleep(RECONNECT_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
