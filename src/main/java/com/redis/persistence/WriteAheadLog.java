package com.redis.persistence;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WriteAheadLog (WAL) — Phase 2, file 1.
 *
 * THE CORE IDEA, BEFORE ANY CODE:
 * Our DataStore lives entirely in RAM. If the process crashes (power cut,
 * kill -9, JVM bug, whatever) everything in DataStore vanishes instantly —
 * RAM is not durable storage. A "Write-Ahead Log" is the standard technique
 * every real database (including real Redis, whose version of this is
 * called the AOF — Append Only File) uses to survive that: before we apply
 * ANY write command (SET, LPUSH, HSET, ...) to the in-memory DataStore, we
 * first durably record that exact command to a file on disk. "Write-ahead"
 * literally means "logged BEFORE being applied" — the log write always
 * happens first.
 *
 * Why does this help? Because after a crash, on restart, we can re-read
 * this file top to bottom and re-run every command in it against a fresh,
 * empty DataStore, ending up in EXACTLY the state we were in right before
 * the crash. The file is a durable, ordered history of "everything that
 * ever changed", and DataStore itself doesn't need to know anything about
 * persistence at all — this class handles reading and writing that history
 * file; wiring "log it, THEN apply it" together happens later, once we have
 * a CommandProcessor + WriteAheadLog both alive together (that wiring will
 * live in Main.java, Phase 3, once we actually have a running server to
 * wire it into).
 *
 * WHAT THIS FILE DOES NOT DO (yet):
 * It doesn't decide WHEN to log something — some other piece of code will
 * call append() for every write command. It also doesn't shrink or rotate
 * itself — an append-only file grows forever until something replaces it
 * with a fresh snapshot; that's SnapshotManager's job, the very next file
 * in this same phase.
 */
public class WriteAheadLog {

    private final Path filePath;

    // Kept open for the lifetime of this WriteAheadLog instance, in APPEND
    // mode — every write always lands at the current end of the file,
    // never overwriting earlier entries. We deliberately do NOT wrap this
    // in a BufferedOutputStream: since we're going to force() (fsync, see
    // below) after every single append anyway, an extra in-JVM buffering
    // layer wouldn't reduce how often we actually touch the disk — it would
    // just add a `flush()` step we'd have to remember to call before every
    // force(). Skipping it keeps the durability guarantee obviously
    // correct, at the cost of raw throughput — a completely reasonable
    // trade for a from-scratch learning project. (A real production system
    // would batch multiple commands into one fsync — "group commit" — we
    // note this as a possible Phase 10 optimization, not something to build
    // now.)
    //
    // We keep TWO references to what is really "one open file": `fileOut`
    // (the raw FileOutputStream) and `out` (a DataOutputStream WRAPPING
    // that same fileOut, adding the writeInt/writeUTF convenience methods
    // used in append() below). DataOutputStream is a "decorator" — it adds
    // behavior on top of an existing stream without replacing it — but it
    // does NOT expose everything the stream underneath it can do. In
    // particular, DataOutputStream has no getFD() method, because "get the
    // OS-level file descriptor" isn't something every possible OutputStream
    // even has (an OutputStream writing to memory, or over the network,
    // has no file descriptor to get). Only FileOutputStream itself knows
    // about the actual file descriptor, so we keep a direct reference to
    // it specifically so append() can call fileOut.getFD().sync() for the
    // fsync step — going through `out` alone cannot reach that method.
    //
    // NOT `final` (a change from how this file started): truncate() below
    // needs to completely close the current file handle and open a fresh
    // one pointing at the same path, once a snapshot has made the old
    // contents redundant. `final` fields can only ever be assigned once —
    // in the constructor — so keeping the WAL truncatable after
    // construction means these two fields can no longer be `final`. This
    // is a deliberate, narrow loss of that safety guarantee: every write to
    // either field still only ever happens while holding `lock`, so the
    // thread-safety story is unaffected, only the "assigned exactly once"
    // guarantee is gone.
    private FileOutputStream fileOut;
    private DataOutputStream out;

    /**
     * PLAIN ReentrantLock (not ReentrantReadWriteLock like DataStore used).
     * Why plain, not read/write? DataStore needed the read/write split
     * because GET (read) massively outnumbers SET (write) in a typical
     * workload, and concurrent reads are genuinely safe together. Here,
     * there is only ONE kind of operation happening while the server is
     * live: appending. There's no "many readers, one writer" split to
     * exploit — every append must be fully serialized relative to every
     * other append anyway, because they're all writing sequential bytes to
     * the SAME file and we need entry N to be completely written before
     * entry N+1 starts, or the file's contents would be corrupted/
     * interleaved garbage. A plain, simple mutex is exactly the right tool
     * when there's no meaningful distinction between "readers" and
     * "writers" to exploit. (Reading the whole file back happens once, at
     * startup during replay(), before the server is even accepting
     * commands yet, so it doesn't need to compete with append() at all in
     * practice — but we still guard it with the same lock for correctness
     * if that assumption ever changes.)
     */
    private final ReentrantLock lock = new ReentrantLock();

    // ===== Dashboard introspection (Persistence section) =====
    // Seeded once at construction by counting whatever's already in the
    // file (a real, on-disk count, not a guess), then incremented on
    // every successful append() from that point on — so this stays a
    // genuine running total across the WAL's whole lifetime, not just
    // "since this process started."
    private volatile long entryCount;
    private volatile long lastWriteAtMillis = -1;

    /**
     * @param filePath where the WAL file lives on disk, e.g.
     *                 Path.of("wal/node1.wal") — matches the wal/ folder
     *                 from the project structure.
     */
    public WriteAheadLog(Path filePath) throws IOException {
        this.filePath = filePath;

        // Files.createDirectories is safe to call even if the directory
        // already exists (unlike File.mkdir(), it won't throw or fail in
        // that case) — this guarantees the wal/ folder exists before we
        // try to create a file inside it, even on a completely fresh
        // checkout of this project where wal/ might be empty or absent.
        Path parentDirectory = filePath.toAbsolutePath().getParent();
        if (parentDirectory != null) {
            Files.createDirectories(parentDirectory);
        }

        // `new FileOutputStream(file, true)` — that second boolean argument
        // is what makes this APPEND mode. Without it, opening a
        // FileOutputStream on an existing file TRUNCATES it to zero bytes
        // immediately — which would silently destroy our entire crash-
        // recovery history the moment the server restarts! This `true` is
        // one of the most important single characters in this whole file.
        this.fileOut = new FileOutputStream(filePath.toFile(), true);
        this.out = new DataOutputStream(this.fileOut);

        // Seed entryCount from whatever's ALREADY durably on disk (e.g.
        // from a previous run) - readAll() is only ever this expensive
        // ONE time, here at startup, never again during normal operation.
        this.entryCount = readAll().size();
    }

    /**
     * Durably appends one command to the log.
     *
     * `List<String> commandArgs` uses the exact same shape CommandProcessor
     * already works with — e.g. ["SET", "foo", "bar"] — so no translation
     * layer is needed between "a command CommandProcessor just handled" and
     * "an entry written to the WAL": the eventual wiring in Main.java can
     * just pass the same List<String> to both.
     *
     * ON-DISK FORMAT (deliberately simple, not RESP):
     * We use DataOutputStream's writeInt/writeUTF, which are built-in JDK
     * methods for writing LENGTH-PREFIXED data:
     *   writeInt(n)      -> writes 4 raw bytes encoding the int n
     *   writeUTF(string) -> writes a 2-byte length prefix, then that many
     *                       bytes of the string's UTF-8 encoding
     * So one WAL entry for ["SET", "foo", "bar"] looks like, conceptually:
     *   [int: 3]  [len:3]"SET"  [len:3]"foo"  [len:3]"bar"
     * Length-prefixing (instead of, say, separating words with spaces) is
     * exactly how we avoid ambiguity: if a value itself CONTAINS a space,
     * or even a newline, none of that matters, because we never scan for a
     * delimiter character — we always know exactly how many bytes to read
     * next because we read the length FIRST. (Real RESP, which you'll see
     * in Phase 3, uses this same length-prefixing idea for exactly this
     * reason.) One real limitation worth knowing: writeUTF's length prefix
     * is only 2 bytes, capping any single string at 65535 bytes — fine for
     * realistic keys/values in this project, but worth knowing if you ever
     * store something huge.
     */
    public void append(List<String> commandArgs) throws IOException {
        lock.lock();
        try {
            out.writeInt(commandArgs.size());
            for (String arg : commandArgs) {
                out.writeUTF(arg);
            }

            // out.flush() pushes any bytes still sitting in Java-level
            // stream buffers out to the underlying OS file descriptor.
            // This is NOT the same as durability yet — see force() below.
            out.flush();

            // ===== fsync: the actual durability guarantee =====
            // Concept, from the ground up: when a program writes to a file,
            // those bytes usually land first in the OPERATING SYSTEM's own
            // page cache in RAM — NOT immediately on the physical disk —
            // purely for performance (RAM writes are vastly faster than
            // disk writes, so the OS batches many small writes together
            // before actually touching the disk hardware). This means that
            // even after out.flush() returns successfully, a power loss or
            // OS crash at that exact moment could still lose the data —
            // it never made it past the OS's RAM cache to the physical
            // disk.
            //
            // FileDescriptor.sync() is Java's way of issuing an "fsync"
            // system call for this file: it blocks until the OS confirms
            // the data (AND the file's metadata, like its new length after
            // this append) has actually been written to physical storage.
            // Only AFTER sync() returns can we honestly say "this command
            // is now crash-durable." This is why WAL writes are
            // comparatively slow compared to just writing to memory — real
            // disk I/O is happening, on purpose, right here, specifically
            // so we can make a durability promise. (You may see other Java
            // code do the equivalent thing via
            // fileOutputStream.getChannel().force(true) instead — that's
            // the NIO-style path to the same underlying fsync syscall; we
            // use the simpler, more direct getFD().sync() here since we
            // don't need any other FileChannel features in this class.)
            fileOut.getFD().sync();
            entryCount++;
            lastWriteAtMillis = System.currentTimeMillis();
        } finally {
            lock.unlock();
        }
    }

    public long getEntryCount() {
        return entryCount;
    }

    /** -1 means no write has happened yet this run (and none was ever recorded before this run's startup count either, for a brand new WAL). */
    public long getLastWriteAtMillis() {
        return lastWriteAtMillis;
    }

    /**
     * Reads every command ever durably appended to this WAL, in the exact
     * order they were written, for crash-recovery replay at startup.
     *
     * Returns a List<List<String>> — a list of commands, each one being a
     * list of words, e.g. [["SET","foo","bar"], ["LPUSH","mylist","x"]] —
     * ready for calling code (Main.java, in Phase 3) to feed one-by-one
     * into a fresh CommandProcessor to rebuild DataStore's state exactly as
     * it was before the crash.
     *
     * We open a SEPARATE, independent FileInputStream for reading here,
     * rather than trying to reuse `out` — reading and writing the same
     * open file handle with two different stream wrappers gets confusing
     * fast (they'd fight over the file's current position), and replay is
     * only ever expected to run once, at startup, before any append() calls
     * for this run of the program have happened yet.
     */
    public List<List<String>> readAll() throws IOException {
        lock.lock();
        try {
            List<List<String>> commands = new ArrayList<>();

            if (!Files.exists(filePath)) {
                // A brand-new node with no prior WAL file at all — nothing
                // to replay, which is a perfectly normal, expected case
                // (e.g. the very first time a node ever starts).
                return commands;
            }

            try (DataInputStream in = new DataInputStream(new FileInputStream(filePath.toFile()))) {
                while (true) {
                    int argCount;
                    try {
                        argCount = in.readInt();
                    } catch (EOFException endOfFile) {
                        // Clean end of file, exactly between two complete
                        // entries — this is the NORMAL way this loop ends
                        // every time replay finishes successfully.
                        break;
                    }

                    List<String> args = new ArrayList<>(argCount);
                    try {
                        for (int i = 0; i < argCount; i++) {
                            args.add(in.readUTF());
                        }
                    } catch (EOFException truncatedEntry) {
                        // ===== The "torn write" case =====
                        // This happens if the process crashed WHILE in the
                        // middle of writing one entry — e.g. we'd already
                        // written the argCount and one or two of the
                        // strings, but the crash hit before the rest of
                        // that entry's bytes made it to disk. That last,
                        // incomplete entry can never be trusted or safely
                        // replayed (we have no way to know what the missing
                        // bytes would have said), so the correct, safe
                        // choice is to DISCARD this partial entry entirely
                        // and stop replay here — treating the log as ending
                        // right before this broken entry started. This is
                        // the exact same reasoning real databases use: an
                        // fsync'd, fully-written entry is trustworthy; a
                        // torn one at the very end of the file after a
                        // crash is simply thrown away, since whatever
                        // command it represented never got fully
                        // acknowledged as durable in the first place.
                        break;
                    }
                    commands.add(args);
                }
            }

            return commands;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Current size of the WAL file in bytes. Not used yet, but the project
     * brief calls for the Phase 8 dashboard to show "WAL size" per node —
     * this is the simple building block for that, exposed now while we're
     * already here rather than needing to come back and add file-size
     * logic later.
     */
    public long sizeInBytes() throws IOException {
        lock.lock();
        try {
            return Files.exists(filePath) ? Files.size(filePath) : 0L;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the underlying file handle. Should be called on a clean
     * shutdown (we'll wire this into Main.java's shutdown handling in
     * Phase 3) so the OS releases the file descriptor properly rather than
     * relying on the JVM/OS to clean it up eventually.
     */
    public void close() throws IOException {
        lock.lock();
        try {
            out.close();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resets this WAL to a completely empty file — used by SnapshotManager
     * (the next file) right after it finishes writing a full snapshot of
     * DataStore. Once every key/value currently in memory has been safely
     * captured in that snapshot, EVERY command previously recorded in this
     * WAL is now redundant: replaying them again on top of that snapshot
     * would just recreate what the snapshot already contains. So instead of
     * letting this file grow forever, we reset it to zero bytes right after
     * a successful snapshot, and only commands that happen AFTER that point
     * get logged into it going forward. (This exact pattern — periodic
     * snapshot + truncate — is why real Redis's AOF file doesn't grow
     * without bound either; it calls this general technique "AOF
     * rewriting.")
     *
     * HOW THIS WORKS: we can't just call `out.truncate()`-style operations
     * mid-stream easily with plain FileOutputStream, so instead we close
     * the current file handle entirely, then open a BRAND NEW
     * FileOutputStream on the same path WITHOUT append mode (the second
     * constructor argument is `false` here, unlike the constructor's
     * `true`) — opening a FileOutputStream in non-append mode is exactly
     * what truncates an existing file to zero bytes. We then immediately
     * hand off to these fresh streams so every append() call after this
     * point continues to work exactly as before, just starting from an
     * empty file.
     */
    public void truncate() throws IOException {
        lock.lock();
        try {
            out.close(); // release the old file handle first
            this.fileOut = new FileOutputStream(filePath.toFile(), false);
            this.out = new DataOutputStream(this.fileOut);
            entryCount = 0; // the file's now genuinely empty - keep this counter honest
        } finally {
            lock.unlock();
        }
    }
}
