package com.redis.persistence;

import com.redis.core.DataStore;
import com.redis.core.RedisValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SnapshotManager — Phase 2, file 2 (last file of this phase).
 *
 * WHY WE NEED THIS ON TOP OF THE WAL:
 * The WAL alone is enough to survive a crash, but replaying it gets slower
 * and slower the longer a server has been running — restarting a node that
 * has been up for months could mean replaying millions of tiny commands one
 * at a time before it's usable again. A SNAPSHOT is a full, point-in-time
 * dump of DataStore's ENTIRE current contents in one compact file. On
 * startup, we can instead load ONE snapshot (fast — it's already the final
 * state, no replaying needed) and then only replay whatever WAL entries
 * were appended AFTER that snapshot was taken. Together, "snapshot + a
 * short WAL tail" is what keeps startup fast no matter how long a node has
 * been alive — this pairing (snapshot + WAL) is exactly how real Redis's
 * RDB+AOF combination works too.
 */
public class SnapshotManager {

    private final Path snapshotFilePath;

    public SnapshotManager(Path snapshotFilePath) throws IOException {
        this.snapshotFilePath = snapshotFilePath;
        Path parentDirectory = snapshotFilePath.toAbsolutePath().getParent();
        if (parentDirectory != null) {
            Files.createDirectories(parentDirectory);
        }
    }

    /**
     * Writes a full snapshot of `dataStore`'s current contents to disk,
     * and — only once that snapshot is safely, durably complete — resets
     * `wal` to empty, since every command it was tracking is now captured
     * in the fresh snapshot.
     *
     * ===== WHY WE HOLD THE WRITE LOCK FOR THE WHOLE SERIALIZATION =====
     * We reuse DataStore.writeTransaction(...) (built back in Phase 1) to
     * run the entire "walk every key, serialize it to disk" process while
     * holding the EXCLUSIVE write lock the whole time. This means, for
     * however long it takes to write this snapshot file, NO client command
     * (GET, SET, LPUSH, anything) can run — the whole server briefly
     * pauses. This is a real, honest trade-off worth naming plainly: real
     * Redis avoids this pause entirely by using the OS's fork() system call
     * to create a copy-on-write child process that dumps the data while the
     * ORIGINAL process keeps serving clients uninterrupted — but
     * implementing that is a deep, OS-specific, process-management topic
     * that's out of scope for this learning project. Holding the write lock
     * for the snapshot's duration is simple and 100% correct (nobody can
     * mutate DataStore mid-write, so the snapshot is always perfectly
     * consistent) at the cost of a brief pause — an entirely reasonable
     * trade for what we're building here, and something we could revisit
     * as a Phase 10 optimization if there's time.
     *
     * ===== THE CHECKED-EXCEPTION-INSIDE-A-LAMBDA PROBLEM =====
     * writeTransaction expects a java.util.function.Function, and
     * Function.apply() is NOT declared to throw IOException (or any
     * checked exception) — that's just how the Function interface is
     * defined in the JDK, and we can't change it. But writing to a file
     * genuinely CAN throw IOException. The standard, idiomatic fix — used
     * throughout the JDK itself — is java.io.UncheckedIOException: a
     * RuntimeException (unchecked, so it satisfies the compiler inside the
     * lambda) that simply WRAPS the original IOException. We throw that
     * from inside the lambda, then immediately catch it right outside
     * writeTransaction(...) and unwrap it back into a plain checked
     * IOException — so from the perspective of anyone calling save(), it
     * still just throws a normal IOException like every other method in
     * this file, and the "unchecked wrapper" trick never leaks out as part
     * of this class's public API.
     */
    public void save(DataStore dataStore, WriteAheadLog wal) throws IOException {
        // Write to a TEMPORARY file first, never directly to the real
        // snapshot path — explained fully in writeSnapshotFile()'s comment
        // below, but in short: this protects us from ever leaving behind a
        // half-written, corrupt "real" snapshot file if the process crashes
        // partway through writing it.
        Path tempFile = snapshotFilePath.resolveSibling(snapshotFilePath.getFileName() + ".tmp");

        try {
            dataStore.writeTransaction(map -> {
                try {
                    writeSnapshotFile(tempFile, map);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to write snapshot to " + tempFile, e);
                }
                return null; // writeTransaction's Function must return something; we don't need a value here.
            });
        } catch (UncheckedIOException wrapper) {
            throw wrapper.getCause();
        }

        // ===== Atomic rename: the crash-safety trick that ties this all together =====
        // Files.move with ATOMIC_MOVE replaces the OLD snapshot file with
        // the NEW one in a single, indivisible filesystem operation — from
        // any outside observer's point of view (including "the process
        // crashes at this exact instant"), the snapshot file at
        // `snapshotFilePath` is EITHER still the complete old version, OR
        // already the complete new version — there is no possible
        // in-between "half old, half new" state ever visible on disk. If we
        // had instead written directly into `snapshotFilePath` and the
        // process crashed midway, we could be left with a truncated,
        // corrupted snapshot and NO way to recover the old good one. Write
        // to a temp file, make sure it's completely done, THEN atomically
        // swap it into place — this is a standard, important pattern for
        // any durable "replace this whole file" operation, not just
        // specific to Redis or this project.
        Files.move(tempFile, snapshotFilePath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);

        // Only NOW — after the new snapshot is fully, durably, atomically
        // in place — is it safe to discard the WAL's history.
        wal.truncate();
    }

    /**
     * The actual file-writing logic, kept as its own method (rather than
     * inline inside the lambda in save()) purely for readability — the
     * lambda in save() would otherwise be a large, deeply-nested block.
     *
     * ON-DISK FORMAT for the snapshot file:
     *   [int: total key count]
     *   for each key:
     *     [UTF: key name]
     *     [UTF: type name, e.g. "STRING"/"LIST"/"HASH"/"SET"]
     *     [long: expireAt timestamp, or -1 if no expiry]
     *     then, depending on type:
     *       STRING -> [UTF: the string value]
     *       LIST   -> [int: element count] then that many [UTF: element]
     *       HASH   -> [int: field count] then that many [UTF: field][UTF: value] pairs
     *       SET    -> [int: member count] then that many [UTF: member]
     *
     * We know the total key count UPFRONT here (map.size()), unlike the
     * WAL's readAll(), which had to loop "until EOF" because it doesn't
     * know in advance how many commands were ever appended. Writing the
     * count first is simpler to read back (a plain counted loop, no
     * EOFException handling needed at all) — a nice contrast worth noticing
     * between the two files in this same phase, both valid techniques for
     * different situations.
     *
     * We also deliberately SKIP any key that is already expired (checked
     * via RedisValue.isExpired(), the same method DataStore's lazy expiry
     * uses) — there's no point saving a key into a snapshot that's already
     * logically dead; real Redis's own RDB snapshots do the same thing.
     */
    private void writeSnapshotFile(Path path, Map<String, RedisValue> map) throws IOException {
        try (FileOutputStream fileOut = new FileOutputStream(path.toFile());
             DataOutputStream out = new DataOutputStream(fileOut)) {

            List<Map.Entry<String, RedisValue>> liveEntries = new ArrayList<>();
            for (Map.Entry<String, RedisValue> entry : map.entrySet()) {
                if (!entry.getValue().isExpired()) {
                    liveEntries.add(entry);
                }
            }

            out.writeInt(liveEntries.size());
            for (Map.Entry<String, RedisValue> entry : liveEntries) {
                writeOneEntry(out, entry.getKey(), entry.getValue());
            }

            out.flush();
            // Same fsync reasoning as WriteAheadLog.append(): without this,
            // the snapshot could still be sitting in the OS's page cache,
            // not physical disk, when we go on to atomically rename it into
            // place in save() above — we want the guarantee that once this
            // method returns, the snapshot's bytes are truly durable.
            fileOut.getFD().sync();
        }
    }

    private void writeOneEntry(DataOutputStream out, String key, RedisValue value) throws IOException {
        out.writeUTF(key);
        out.writeUTF(value.getType().name());
        out.writeLong(value.getExpireAt());

        switch (value.getType()) {
            case STRING -> out.writeUTF(value.asString());
            case LIST -> {
                List<String> list = value.asList();
                out.writeInt(list.size());
                for (String element : list) {
                    out.writeUTF(element);
                }
            }
            case HASH -> {
                Map<String, String> hash = value.asHash();
                out.writeInt(hash.size());
                for (Map.Entry<String, String> field : hash.entrySet()) {
                    out.writeUTF(field.getKey());
                    out.writeUTF(field.getValue());
                }
            }
            case SET -> {
                Set<String> set = value.asSet();
                out.writeInt(set.size());
                for (String member : set) {
                    out.writeUTF(member);
                }
            }
        }
    }

    /**
     * Loads a previously-saved snapshot directly into `dataStore`, restoring
     * every key, its type, its value, and its expiry timestamp exactly as
     * they were saved.
     *
     * We use plain dataStore.set(...) calls here rather than going through
     * writeTransaction — this is safe specifically because load() is only
     * ever meant to be called once, at startup, BEFORE the server has
     * started accepting any client commands yet (that wiring happens in
     * Main.java, Phase 3). There's no concurrent access to race against
     * yet, so the simpler per-key set() calls are perfectly fine here, and
     * more readable than wrapping the whole load in one giant transaction
     * for no real benefit.
     */
    public void loadInto(DataStore dataStore) throws IOException {
        if (!Files.exists(snapshotFilePath)) {
            // No snapshot exists yet — completely normal for a node's very
            // first-ever startup. Nothing to load; DataStore stays empty,
            // and Main.java (Phase 3) will fall back to replaying the WAL
            // from the beginning instead.
            return;
        }

        try (DataInputStream in = new DataInputStream(new FileInputStream(snapshotFilePath.toFile()))) {
            int keyCount = in.readInt();
            for (int i = 0; i < keyCount; i++) {
                String key = in.readUTF();
                RedisValue.RedisType type = RedisValue.RedisType.valueOf(in.readUTF());
                long expireAt = in.readLong();

                RedisValue value = switch (type) {
                    case STRING -> RedisValue.ofString(in.readUTF());
                    case LIST -> {
                        int count = in.readInt();
                        List<String> list = new LinkedList<>();
                        for (int j = 0; j < count; j++) {
                            list.add(in.readUTF());
                        }
                        yield RedisValue.ofList(list);
                    }
                    case HASH -> {
                        int count = in.readInt();
                        Map<String, String> hash = new HashMap<>();
                        for (int j = 0; j < count; j++) {
                            String field = in.readUTF();
                            String fieldValue = in.readUTF();
                            hash.put(field, fieldValue);
                        }
                        yield RedisValue.ofHash(hash);
                    }
                    case SET -> {
                        int count = in.readInt();
                        Set<String> set = new LinkedHashSet<>();
                        for (int j = 0; j < count; j++) {
                            set.add(in.readUTF());
                        }
                        yield RedisValue.ofSet(set);
                    }
                };

                if (expireAt != -1L) {
                    value.setExpireAtTimestamp(expireAt);
                }
                dataStore.set(key, value);
            }
        }
    }
}
