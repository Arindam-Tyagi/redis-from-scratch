package com.redis.network;

import com.redis.core.CommandProcessor;
import com.redis.core.DataStore;
import com.redis.persistence.WriteAheadLog;
import com.redis.raft.RaftNode;
import com.redis.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Server — Phase 3, file 3. Owns the "listening" side of networking: opens
 * one port, waits for incoming client connections forever, and hands each
 * one off to its own ClientHandler running on its own virtual thread.
 *
 * ===== ServerSocket vs Socket — the distinction that matters =====
 * We met `Socket` in ClientHandler.java — it represents ONE already-
 * established connection to ONE specific client. `ServerSocket` is a
 * different, earlier stage: it represents a LISTENING PORT itself, the
 * thing waiting for NEW clients to show up in the first place. The
 * relationship is: you create exactly ONE ServerSocket bound to one port
 * (e.g. 6379) for the lifetime of your server, and then you repeatedly call
 * `serverSocket.accept()` — which BLOCKS (pauses this thread, doing
 * nothing, using essentially no CPU) until a new client actually connects,
 * and then returns a brand-new `Socket` object representing THAT one
 * client's connection. You then go back and call accept() again to wait
 * for the NEXT client, forever, in a loop — that loop is this entire
 * class's reason to exist.
 *
 * ===== Why we need a thread per client at all =====
 * `accept()` only ever gives us ONE new Socket at a time, and while a
 * ClientHandler is busy actually talking to a client (reading a command,
 * maybe writing to the WAL, computing a reply, writing it back), that
 * could take some non-zero amount of time. If we tried to handle that
 * conversation directly on the SAME thread that calls accept() in a loop,
 * we could only ever serve ONE client at a time — a second client trying
 * to connect while we're busy with the first would simply have to wait,
 * potentially for a long time, which is unacceptable for a server meant to
 * handle many concurrent clients (remember, our project brief calls for 3
 * nodes serving many clients each). The fix: the accept loop's ONLY job is
 * to accept connections and immediately hand each one off to run
 * INDEPENDENTLY on its own thread, then go straight back to accept() again
 * to wait for the next one — accepting is never blocked by any one
 * client's actual command processing.
 *
 * ===== Virtual threads, finally explained in full (teased since pom.xml) =====
 * A "platform thread" (Java's traditional Thread, and what you get from
 * `new Thread(...)`) maps 1-to-1 onto a real operating system thread — a
 * genuinely heavyweight OS resource, typically reserving around 1MB of
 * stack memory each and requiring the OS kernel to manage its scheduling.
 * Creating thousands of platform threads (e.g. one per connected client,
 * if we had thousands of clients) can genuinely exhaust memory or make the
 * OS scheduler struggle, long before we've done any real work.
 *
 * A "virtual thread" (`Thread.ofVirtual()`, stable since Java 21 — exactly
 * why our pom.xml pins Java 21) is a thread MANAGED BY THE JVM ITSELF,
 * not directly by the OS. The JVM runs many virtual threads on top of a
 * small pool of real OS threads, transparently pausing/resuming a virtual
 * thread whenever it's blocked waiting on something (like our
 * `parser.parseCommand()` call sitting there waiting for a slow client to
 * send its next command, or `serverSocket.accept()` itself waiting for a
 * new connection) — while it's blocked like that, the underlying real OS
 * thread is freed up to go do other virtual threads' work instead, then
 * comes back once there's actually something to do. The practical result:
 * we can create ONE virtual thread PER CLIENT CONNECTION — even thousands
 * of them — and each one gets to be written as simple, ordinary,
 * sequential blocking code (exactly what ClientHandler.run()'s while-loop
 * is) without needing complicated asynchronous/callback-based programming
 * to scale, and without exhausting real OS resources the way thousands of
 * platform threads would.
 */
public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final int port;
    private final CommandProcessor commandProcessor;
    private final WriteAheadLog wal;

    // ===== Phase 6 additions ===== (just passed straight through to every
    // ClientHandler this Server creates — see ClientHandler.java for what
    // each one is actually used for)
    private final DataStore dataStore;
    private final ReplicationManager replicationManager;

    // ===== Phase 7 addition =====
    // Null on a node not running Raft at all (kept only for backward
    // compatibility with any earlier, pre-Phase-7 setup); non-null on
    // every one of our 9 Phase 7 nodes. Passed straight through to every
    // ClientHandler this Server creates - see ClientHandler.java for
    // exactly how it changes write-command handling.
    private final RaftNode raftNode;

    public Server(int port, CommandProcessor commandProcessor, WriteAheadLog wal,
                   DataStore dataStore, ReplicationManager replicationManager, RaftNode raftNode) {
        this.port = port;
        this.commandProcessor = commandProcessor;
        this.wal = wal;
        this.dataStore = dataStore;
        this.replicationManager = replicationManager;
        this.raftNode = raftNode;
    }

    /**
     * Starts listening and blocks the calling thread FOREVER, accepting
     * and dispatching client connections one after another, until the
     * process is killed or an unrecoverable I/O error occurs. Main.java
     * (the next and final file of this phase) will call this as
     * essentially the very last thing it does.
     *
     * try-with-resources on ServerSocket: if this method ever exits (an
     * exception escapes the while loop below), the listening port itself
     * gets released automatically — important so that restarting the
     * process afterward doesn't fail with "port already in use" from a
     * ServerSocket we forgot to close.
     */
    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            logger.info("Server listening on port {}", port);

            while (true) {
                // Blocks here — using effectively no CPU — until a client
                // connects. This is the accept loop's entire job, repeated
                // forever.
                Socket clientSocket = serverSocket.accept();
                logger.info("Accepted connection from {}", clientSocket.getRemoteSocketAddress());

                ClientHandler handler = new ClientHandler(clientSocket, commandProcessor, wal, dataStore, replicationManager, raftNode);

                // Thread.ofVirtual() returns a BUILDER (a small object
                // whose only purpose is to configure and then create
                // something — you'll see this "builder" shape in other
                // Java APIs too). `.start(handler)` both creates the
                // virtual thread AND immediately starts it running
                // `handler.run()` — all in one call. We deliberately don't
                // keep a reference to the returned Thread object: we never
                // need to check on it, wait for it, or interrupt it later
                // from here — once started, that thread lives entirely on
                // its own until ClientHandler.run() returns (the client
                // disconnected or errored out), then it simply ends and
                // the JVM reclaims its resources automatically.
                Thread.ofVirtual().start(handler);
            }
        }
    }
}
