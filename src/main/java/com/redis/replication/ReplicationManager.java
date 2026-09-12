package com.redis.replication;

import com.redis.core.CommandProcessor;
import com.redis.core.DataStore;
import com.redis.core.RedisValue;
import com.redis.network.RESPParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ReplicationManager — Phase 6's first file. Lives on a PRIMARY node only.
 * Its job: keep track of every currently-connected replica, and whenever a
 * write happens on this primary, forward that exact same command to every
 * replica so they can apply it too and stay in sync.
 *
 * ===== The overall replication design (read this before the code) =====
 * When a replica starts up, it connects to its primary as an ordinary
 * client and sends one special command: SYNC (wired up in ClientHandler,
 * a later file in this phase). The primary responds in two parts, over
 * that SAME connection:
 *   1. A ONE-TIME "full sync": a burst of ordinary write commands (SET,
 *      RPUSH, HSET, SADD, PEXPIRE) that, if replayed in order against an
 *      EMPTY DataStore, reconstruct the primary's entire current dataset.
 *      buildFullSyncCommands() below builds exactly this list.
 *   2. From then on, forever: every NEW write command the primary
 *      receives from any client gets forwarded down this same connection,
 *      live, the moment it's applied — this is what propagate() does.
 * The replica just keeps reading commands off that connection and
 * applying them to its own DataStore, never distinguishing "catch-up"
 * commands from "live" ones — they're both just commands to apply, in
 * order. This is a real simplification of what real Redis actually does
 * (real Redis's full resync sends a compact RDB-format binary snapshot,
 * not a list of commands) — we reuse plain commands instead because we
 * already have a fully working, tested pipeline for encoding and applying
 * commands (RESPParser + CommandProcessor), so there's no new wire format
 * to invent. The tradeoff is that a full sync becomes slower and larger
 * for a dataset with a huge number of keys — a completely reasonable
 * tradeoff at this project's scale, and a natural Phase 10 optimization
 * if ever needed.
 *
 * ===== CopyOnWriteArrayList — a new concurrency concept =====
 * We need a list of connected replicas' OutputStreams that's safe to read
 * from and write to across many threads at once: every client-handling
 * thread that processes a write command calls propagate(), all
 * potentially at the same time, while a NEW replica connecting adds
 * itself to the same list, and a disconnecting replica gets removed from
 * it — all concurrently.
 *
 * A plain ArrayList would need a lock around every read AND every write to
 * be safe (like DataStore's ReentrantReadWriteLock). CopyOnWriteArrayList
 * takes a different, deliberately lopsided approach: every WRITE
 * (add/remove) makes a brand new copy of the entire underlying array and
 * atomically swaps it in, while every READ (including iterating with a
 * for-each loop, exactly what propagate() does below) needs NO locking at
 * all — it just reads whatever array reference was current when the read
 * started, safely, even if another thread replaces the array underneath
 * it a moment later. This makes writes relatively expensive (a full array
 * copy every time) but reads essentially free and impossible to corrupt.
 * That's exactly the right tradeoff here: replicas connect/disconnect
 * RARELY, but propagate() gets called on EVERY single write command,
 * potentially from many client threads simultaneously — we want that hot
 * path to be as cheap and simple as possible, and can easily afford an
 * occasional array copy on the rare event of a replica joining or leaving.
 */
public class ReplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    private final List<OutputStream> replicaStreams = new CopyOnWriteArrayList<>();

    /**
     * Called once, right after a replica finishes its full sync (see
     * ClientHandler's SYNC handling) — from this point on, this replica's
     * connection will receive every future write command via propagate().
     */
    public void registerReplica(OutputStream replicaOutputStream) {
        replicaStreams.add(replicaOutputStream);
        logger.info("Replica connected - now streaming writes to {} replica(s)", replicaStreams.size());
    }

    /**
     * Handles a brand-new replica's SYNC request END TO END: builds the
     * full-dataset command list, writes every one of those commands
     * DIRECTLY to this one replica's stream (NOT via propagate(), which
     * broadcasts to already-registered replicas — this one isn't
     * registered yet, deliberately, so it can't possibly receive a LIVE
     * write out of order while the full sync is still being sent), and
     * only registers it for future live updates once the full sync has
     * been completely written out. Called by ClientHandler the moment it
     * sees a SYNC command arrive on a client connection.
     */
    public void performFullSync(OutputStream replicaOutputStream, DataStore dataStore) throws IOException {
        List<List<String>> fullSyncCommands = buildFullSyncCommands(dataStore);
        logger.info("Sending full sync ({} command(s)) to new replica", fullSyncCommands.size());
        for (List<String> command : fullSyncCommands) {
            RESPParser.writeReply(replicaOutputStream, toWireFormat(command));
        }
        registerReplica(replicaOutputStream);
    }

    /**
     * Forwards one write command to every currently-connected replica.
     * Called by ClientHandler right after a write command has been
     * successfully applied to THIS node's own DataStore (see
     * ClientHandler's revision later in this phase) — so a command that
     * FAILED (e.g. a WRONGTYPE error) never gets propagated, keeping
     * replicas from ever seeing a command their primary itself rejected.
     *
     * NOTE ON ORDERING: if two different clients send write commands to
     * this primary at almost the exact same instant, on two different
     * threads, there's no strict guarantee those two propagate() calls
     * reach a replica in exactly the same order they were applied
     * locally. Real production replication systems solve this with a
     * single global sequence number per write; we're accepting this as a
     * known simplification for this project's scope, exactly like the
     * cross-slot multi-key limitation noted in CommandProcessor (Phase 5).
     */
    public void propagate(List<String> commandArgs) {
        if (replicaStreams.isEmpty()) {
            return; // nobody to send to - skip the encoding work entirely
        }

        // We reuse RESPParser.writeReply here in a slightly unusual way:
        // it was built to write REPLIES (CommandResult), but a Redis
        // COMMAND on the wire is ALSO just a RESP array of bulk strings —
        // the exact same shape as an ARRAY-type CommandResult whose items
        // are all BULK_STRINGs. So wrapping our command's words as bulk
        // strings inside an ARRAY CommandResult and handing that to the
        // exact same writeReply() method produces exactly the bytes a
        // real command looks like on the wire — no new encoding method
        // needed, and this is exactly what RESPParser.parseCommand() on
        // the receiving (replica) end already knows how to read back.
        CommandProcessor.CommandResult encodedCommand = toWireFormat(commandArgs);

        for (OutputStream replicaOutputStream : replicaStreams) {
            try {
                RESPParser.writeReply(replicaOutputStream, encodedCommand);
            } catch (IOException e) {
                // This replica's connection is broken (it crashed, its
                // network dropped, etc.) - log it and drop it from the
                // list so we stop wasting time trying to write to a dead
                // connection on every future write. We do NOT treat this
                // as a fatal error for the write itself: the write already
                // succeeded on THIS node, which is what the client is
                // waiting to hear back about.
                logger.warn("Replica connection failed while propagating a write - removing it", e);
                replicaStreams.remove(replicaOutputStream);
            }
        }
    }

    private static CommandProcessor.CommandResult toWireFormat(List<String> args) {
        List<CommandProcessor.CommandResult> items = new ArrayList<>(args.size());
        for (String arg : args) {
            items.add(CommandProcessor.CommandResult.bulkString(arg));
        }
        return CommandProcessor.CommandResult.array(items);
    }

    /**
     * Builds the list of commands a brand-new replica needs to replay, in
     * order, to reconstruct this primary's ENTIRE current dataset from
     * empty. Runs under DataStore's read lock (via readTransaction) for
     * the whole scan, so it sees one single consistent point-in-time
     * picture — no key can be half-added or half-removed partway through
     * building this list, even though other threads may be actively
     * reading/writing DataStore concurrently.
     */
    public List<List<String>> buildFullSyncCommands(DataStore dataStore) {
        return dataStore.readTransaction(map -> {
            List<List<String>> commands = new ArrayList<>();
            for (Map.Entry<String, RedisValue> entry : map.entrySet()) {
                String key = entry.getKey();
                RedisValue value = entry.getValue();

                // Don't bother replicating a key that's already expired -
                // it would just be immediately eligible for removal on
                // the replica too (via its own lazy/active expiry), so
                // sending it over would be pure waste.
                if (value.isExpired()) {
                    continue;
                }

                switch (value.getType()) {
                    case STRING -> commands.add(List.of("SET", key, value.asString()));
                    case LIST -> {
                        List<String> list = value.asList();
                        if (!list.isEmpty()) {
                            List<String> rpush = new ArrayList<>();
                            rpush.add("RPUSH");
                            rpush.add(key);
                            rpush.addAll(list); // RPUSH preserves order exactly - no reversal like LPUSH would need
                            commands.add(rpush);
                        }
                    }
                    case HASH -> {
                        Map<String, String> hash = value.asHash();
                        if (!hash.isEmpty()) {
                            List<String> hset = new ArrayList<>();
                            hset.add("HSET");
                            hset.add(key);
                            for (Map.Entry<String, String> field : hash.entrySet()) {
                                hset.add(field.getKey());
                                hset.add(field.getValue());
                            }
                            commands.add(hset);
                        }
                    }
                    case SET -> {
                        Set<String> set = value.asSet();
                        if (!set.isEmpty()) {
                            List<String> sadd = new ArrayList<>();
                            sadd.add("SADD");
                            sadd.add(key);
                            sadd.addAll(set);
                            commands.add(sadd);
                        }
                    }
                }

                // Re-attach the key's expiry, if it has one, AFTER the
                // command(s) above that create it - PEXPIRE requires the
                // key to already exist. We recompute the REMAINING time
                // (rather than sending the original absolute deadline)
                // because the replica doesn't share the primary's exact
                // "expireAt" clock reading - only how much time is left
                // from right now is meaningful to send across.
                if (value.hasExpiry()) {
                    long remainingMillis = value.getExpireAt() - System.currentTimeMillis();
                    if (remainingMillis > 0) {
                        commands.add(List.of("PEXPIRE", key, String.valueOf(remainingMillis)));
                    }
                    // If remainingMillis <= 0, the key is already expired
                    // by now (a race between our isExpired() check above
                    // and this line) - simplest correct thing is to just
                    // not send an expiry command for it at all; it'll be
                    // cleaned up by the replica's own expiry mechanisms
                    // shortly regardless.
                }
            }
            return commands;
        });
    }
}
