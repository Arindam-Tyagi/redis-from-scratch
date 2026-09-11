package com.redis.network;

import com.redis.core.CommandProcessor;
import com.redis.core.CommandProcessor.CommandResult;
import com.redis.persistence.WriteAheadLog;
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

    public ClientHandler(Socket clientSocket, CommandProcessor commandProcessor, WriteAheadLog wal) {
        this.clientSocket = clientSocket;
        this.commandProcessor = commandProcessor;
        this.wal = wal;
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

                CommandResult result = handleOneCommand(args);
                RESPParser.writeReply(out, result);
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
}
