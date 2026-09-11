package com.redis.core;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * CommandProcessor is the "brain" that sits between whatever brings in a
 * command (later, the network layer in Phase 3) and DataStore. It takes a
 * command already split into words — e.g. ["SET", "foo", "bar"] or
 * ["LRANGE", "mylist", "0", "-1"] — figures out which Redis command that is,
 * validates the arguments, calls the right DataStore method(s), and returns
 * a CommandResult describing what to reply.
 *
 * WHY THE NETWORK LAYER ISN'T INVOLVED YET:
 * We deliberately keep this class 100% independent of sockets, RESP byte
 * encoding, or anything network-related — that's Phase 3's job (RESPParser
 * will turn raw bytes INTO a List<String> like the ones this class expects;
 * ClientHandler will turn a CommandResult BACK INTO raw RESP bytes to send
 * to the client). This separation means CommandProcessor can be tested with
 * plain Java lists and no real network connection at all, and later, if we
 * ever wanted a totally different transport (e.g. a Unix socket, an HTTP
 * API), this class wouldn't need to change one line.
 */
public class CommandProcessor {

    private final DataStore dataStore;

    public CommandProcessor(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Every command name that MUTATES DataStore in some way. ClientHandler
     * (Phase 3, next file) needs this so it can log a command to the
     * WriteAheadLog BEFORE handing it to process() — logging read-only
     * commands like GET/PING/EXISTS/TYPE/LLEN/etc. to the WAL would be
     * pure waste: replaying a GET on startup would do nothing to
     * DataStore's state, so there is nothing worth making durable there.
     *
     * A `Set.of(...)` here is IMMUTABLE — a well-known Java gotcha worth
     * naming explicitly: calling .add(...) or .remove(...) on the Set this
     * returns would throw UnsupportedOperationException at runtime, not a
     * compile error. That's actually exactly what we want here: this list
     * of write commands is fixed, shared, and must never be accidentally
     * mutated by whatever code holds onto a reference to it.
     */
    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "DEL", "LPUSH", "RPUSH", "LPOP", "RPOP", "HSET", "HDEL", "SADD", "SREM"
    );

    /**
     * @return true if `commandName` mutates DataStore when it runs (and
     *         therefore needs to be durably logged before being applied),
     *         false for read-only commands (PING, GET, EXISTS, TYPE, LLEN,
     *         LRANGE, HGET, HGETALL, SMEMBERS, SISMEMBER) or unrecognized
     *         command names.
     *
     * `commandName.toUpperCase(Locale.ROOT)` mirrors process()'s own
     * normalization — a caller can safely pass a command name in any
     * casing ("set", "SET", "sEt") and get the correct answer, exactly
     * like the actual command dispatch does.
     */
    public static boolean isWriteCommand(String commandName) {
        return WRITE_COMMANDS.contains(commandName.toUpperCase(Locale.ROOT));
    }

    /**
     * Processes one command and returns the result to reply with.
     *
     * `List<String> args` is the whole command line as separate words,
     * command name included at index 0 — this mirrors exactly how RESP
     * arrays arrive on the wire (an array of bulk strings), which is why
     * RESPParser in Phase 3 will hand us data in this exact shape.
     */
    public CommandResult process(List<String> args) {
        if (args == null || args.isEmpty()) {
            return CommandResult.error("ERR empty command");
        }

        // Redis commands are case-insensitive ("set", "SET", "SeT" all work),
        // so we normalize to uppercase once, here, rather than making every
        // branch below do its own case-insensitive comparison.
        String commandName = args.get(0).toUpperCase(Locale.ROOT);

        // `subList` gives a VIEW into the same backing list starting after
        // the command name — it does NOT copy the list. This is fine and
        // efficient here because every command method below only reads
        // from `arguments`, never mutates it.
        List<String> arguments = args.subList(1, args.size());

        try {
            // A `switch` EXPRESSION (Java 14+; different from the older
            // `switch` STATEMENT you may have seen). The `->` arrow form
            // means: no `break;` needed, no fall-through between cases (a
            // classic source of bugs in old-style switch statements), and
            // the whole switch itself evaluates to a value that gets
            // returned directly. Each branch here calls a small private
            // method that does the real work for that one command — this
            // keeps `process()` itself readable as a single dispatch table.
            return switch (commandName) {
                case "PING" -> ping(arguments);
                case "SET" -> set(arguments);
                case "GET" -> get(arguments);
                case "DEL" -> del(arguments);
                case "EXISTS" -> exists(arguments);
                case "TYPE" -> type(arguments);
                case "LPUSH" -> push(arguments, true);
                case "RPUSH" -> push(arguments, false);
                case "LPOP" -> pop(arguments, true);
                case "RPOP" -> pop(arguments, false);
                case "LRANGE" -> lrange(arguments);
                case "LLEN" -> llen(arguments);
                case "HSET" -> hset(arguments);
                case "HGET" -> hget(arguments);
                case "HDEL" -> hdel(arguments);
                case "HGETALL" -> hgetall(arguments);
                case "SADD" -> sadd(arguments);
                case "SREM" -> srem(arguments);
                case "SMEMBERS" -> smembers(arguments);
                case "SISMEMBER" -> sismember(arguments);
                default -> CommandResult.error("ERR unknown command '" + commandName + "'");
            };
        } catch (RedisValue.WrongTypeException e) {
            // Thrown by RedisValue.asString()/asList()/asHash()/asSet() when
            // a command is used against a key holding a different type
            // (e.g. LPUSH on a key that holds a plain string). We catch it
            // ONE time, centrally, here — rather than in every single
            // command method — and turn it into the error reply Redis
            // clients expect. e.getMessage() already contains the full
            // "WRONGTYPE ..." text we built into that exception in
            // RedisValue.java.
            return CommandResult.error(e.getMessage());
        } catch (NumberFormatException e) {
            // Thrown by Integer.parseInt(...)/Long.parseLong(...) when a
            // command expects a numeric argument (like LRANGE's start/stop
            // indices) but got something that isn't a valid number.
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    // ==================== Connection / misc ====================

    private CommandResult ping(List<String> args) {
        if (args.isEmpty()) {
            return CommandResult.simpleString("PONG");
        }
        if (args.size() == 1) {
            // Real Redis's PING echoes back a single optional argument
            // instead of replying PONG when one is given.
            return CommandResult.bulkString(args.get(0));
        }
        return wrongArgs("ping");
    }

    // ==================== STRING commands ====================
    // These use DataStore's plain get()/set()/delete()/exists() directly —
    // NOT readTransaction/writeTransaction — because a String's contents
    // can never be mutated after creation. The only mutable thing here is
    // WHICH String object a key points to, and that's exactly what
    // DataStore.set()'s single atomic store.put(key, value) under the
    // write lock already protects.

    private CommandResult set(List<String> args) {
        if (args.size() != 2) {
            return wrongArgs("set");
        }
        String key = args.get(0);
        String value = args.get(1);
        dataStore.set(key, RedisValue.ofString(value));
        return CommandResult.simpleString("OK");
    }

    private CommandResult get(List<String> args) {
        if (args.size() != 1) {
            return wrongArgs("get");
        }
        String key = args.get(0);
        return dataStore.get(key)
                .map(value -> CommandResult.bulkString(value.asString()))
                .orElse(CommandResult.nilBulkString());
    }

    private CommandResult del(List<String> args) {
        if (args.isEmpty()) {
            return wrongArgs("del");
        }
        int deletedCount = 0;
        for (String key : args) {
            if (dataStore.delete(key)) {
                deletedCount++;
            }
        }
        return CommandResult.integer(deletedCount);
    }

    private CommandResult exists(List<String> args) {
        if (args.isEmpty()) {
            return wrongArgs("exists");
        }
        // Real Redis counts a key AGAIN for every time it's named — e.g.
        // EXISTS foo foo returns 2 if foo exists — so we sum booleans
        // rather than de-duplicating the argument list first.
        int count = 0;
        for (String key : args) {
            if (dataStore.exists(key)) {
                count++;
            }
        }
        return CommandResult.integer(count);
    }

    private CommandResult type(List<String> args) {
        if (args.size() != 1) {
            return wrongArgs("type");
        }
        return dataStore.get(args.get(0))
                .map(value -> CommandResult.simpleString(value.getType().name().toLowerCase(Locale.ROOT)))
                .orElse(CommandResult.simpleString("none"));
    }

    // ==================== LIST commands ====================
    // All of these go through dataStore.writeTransaction/readTransaction so
    // that "find the RedisValue, get its List, then read/mutate that List"
    // happens as ONE lock-protected step, never as separate unprotected
    // steps (see the long comment in DataStore.java explaining exactly why).

    private CommandResult push(List<String> args, boolean pushToFront) {
        String commandNameForError = pushToFront ? "lpush" : "rpush";
        if (args.size() < 2) {
            return wrongArgs(commandNameForError);
        }
        String key = args.get(0);
        List<String> valuesToPush = args.subList(1, args.size());

        int newLength = dataStore.writeTransaction(map -> {
            RedisValue existing = map.get(key);
            List<String> list;
            if (existing == null) {
                // LinkedList, not ArrayList: LPUSH/LPOP need to add/remove
                // at the FRONT of the list. ArrayList's add(0, x)/remove(0)
                // are O(n) — every element has to shift over by one. A
                // LinkedList (a doubly-linked list) adds/removes at either
                // end in O(1), which matches exactly how Redis's own list
                // type is optimized for push/pop at both ends. LRANGE
                // (indexed access) is O(n) either way on a LinkedList, but
                // that's an acceptable tradeoff since LPUSH/LPOP are the
                // hot-path operations for this data type.
                list = new LinkedList<>();
                map.put(key, RedisValue.ofList(list));
            } else {
                // asList() throws RedisValue.WrongTypeException here if
                // `key` holds a STRING/HASH/SET instead — exactly the
                // check we want, and it propagates up to process()'s
                // catch block automatically.
                list = existing.asList();
            }

            for (String value : valuesToPush) {
                if (pushToFront) {
                    // Real Redis's LPUSH key a b c results in list [c, b, a]
                    // — each value is pushed to the very front IN ORDER, so
                    // the LAST value given ends up as the new head. Adding
                    // each one at index 0 in a simple loop naturally
                    // produces exactly that order.
                    list.add(0, value);
                } else {
                    // RPUSH key a b c results in [a, b, c] — appended to the
                    // tail in the order given.
                    list.add(value);
                }
            }
            return list.size();
        });

        return CommandResult.integer(newLength);
    }

    private CommandResult pop(List<String> args, boolean popFromFront) {
        String commandNameForError = popFromFront ? "lpop" : "rpop";
        if (args.size() != 1) {
            return wrongArgs(commandNameForError);
        }
        String key = args.get(0);

        String removed = dataStore.writeTransaction(map -> {
            RedisValue existing = map.get(key);
            if (existing == null) {
                return null;
            }
            List<String> list = existing.asList();
            if (list.isEmpty()) {
                // Shouldn't normally happen (see the "remove key when empty"
                // logic below — an empty list is never left sitting in the
                // map), but guarding here costs nothing and avoids a crash
                // if that invariant is ever violated by future code.
                return null;
            }
            String value = popFromFront ? list.remove(0) : list.remove(list.size() - 1);
            if (list.isEmpty()) {
                // Matches real Redis: once a list becomes empty, the KEY
                // ITSELF disappears — EXISTS mylist becomes false, not
                // "true but pointing at an empty list."
                map.remove(key);
            }
            return value;
        });

        return CommandResult.bulkString(removed); // bulkString(null) == nil, which is exactly right when there was nothing to pop
    }

    private CommandResult lrange(List<String> args) {
        if (args.size() != 3) {
            return wrongArgs("lrange");
        }
        String key = args.get(0);
        int start = Integer.parseInt(args.get(1));
        int stop = Integer.parseInt(args.get(2));

        List<String> slice = dataStore.readTransaction(map -> {
            RedisValue existing = map.get(key);
            if (existing == null) {
                return List.<String>of(); // empty, immutable list
            }
            List<String> list = existing.asList();
            int size = list.size();

            // Redis-style negative indices: -1 means "last element", -2
            // "second-to-last", and so on — exactly like Python slicing.
            // We convert negative indices into their positive equivalent
            // first, then clamp both ends into the valid [0, size] range so
            // an out-of-range request (e.g. stop=9999 on a 3-element list)
            // degrades gracefully to "as much as exists" instead of
            // throwing an IndexOutOfBoundsException.
            int from = normalizeIndex(start, size);
            int to = normalizeIndex(stop, size);
            if (from > to || from >= size || size == 0) {
                return List.<String>of();
            }
            to = Math.min(to, size - 1);
            // subList's upper bound is EXCLUSIVE, hence "to + 1". We copy
            // into a new ArrayList (rather than returning the subList view
            // directly) so the returned list is safe to use after this
            // lambda — and after the lock — returns, per the "never let a
            // live collection escape the transaction" rule from
            // DataStore.java.
            return new ArrayList<>(list.subList(from, to + 1));
        });

        return toBulkStringArray(slice);
    }

    private int normalizeIndex(int index, int size) {
        return index < 0 ? Math.max(size + index, 0) : index;
    }

    private CommandResult llen(List<String> args) {
        if (args.size() != 1) {
            return wrongArgs("llen");
        }
        String key = args.get(0);
        int length = dataStore.readTransaction(map -> {
            RedisValue existing = map.get(key);
            return existing == null ? 0 : existing.asList().size();
        });
        return CommandResult.integer(length);
    }

    // ==================== HASH commands ====================

    private CommandResult hset(List<String> args) {
        // HSET key field1 value1 [field2 value2 ...] — after the key, the
        // remaining arguments must come in field/value PAIRS, so there must
        // be an odd total count (key + even number of field/value args) and
        // at least 3 arguments overall (key + one pair).
        if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
            return wrongArgs("hset");
        }
        String key = args.get(0);

        int newFieldsAdded = dataStore.writeTransaction(map -> {
            RedisValue existing = map.get(key);
            Map<String, String> hash;
            if (existing == null) {
                hash = new java.util.HashMap<>();
                map.put(key, RedisValue.ofHash(hash));
            } else {
                hash = existing.asHash();
            }
            int added = 0;
            for (int i = 1; i < args.size(); i += 2) {
                String field = args.get(i);
                String value = args.get(i + 1);
                // Map.put returns the PREVIOUS value for that key, or null
                // if the field is brand new — that's exactly how we count
                // "how many NEW fields were added" versus fields that were
                // merely updated, matching real Redis's HSET return value.
                if (hash.put(field, value) == null) {
                    added++;
                }
            }
            return added;
        });

        return CommandResult.integer(newFieldsAdded);
    }

    private CommandResult hget(List<String> args) {
        if (args.size() != 2) {
            return wrongArgs("hget");
        }
        String key = args.get(0);
        String field = args.get(1);
        String value = dataStore.readTransaction(map -> {
            RedisValue existing = map.get(key);
            return existing == null ? null : existing.asHash().get(field);
        });
        return CommandResult.bulkString(value);
    }

    private CommandResult hdel(List<String> args) {
        if (args.size() < 2) {
            return wrongArgs("hdel");
        }
        String key = args.get(0);
        List<String> fields = args.subList(1, args.size());

        int removedCount = dataStore.writeTransaction(map -> {
            RedisValue existing = map.get(key);
            if (existing == null) {
                return 0;
            }
            Map<String, String> hash = existing.asHash();
            int removed = 0;
            for (String field : fields) {
                if (hash.remove(field) != null) {
                    removed++;
                }
            }
            if (hash.isEmpty()) {
                map.remove(key); // same "no empty containers left behind" rule as lists
            }
            return removed;
        });

        return CommandResult.integer(removedCount);
    }

    private CommandResult hgetall(List<String> args) {
        if (args.size() != 1) {
            return wrongArgs("hgetall");
        }
        String key = args.get(0);

        List<String> flattened = dataStore.readTransaction(map -> {
            RedisValue existing = map.get(key);
            if (existing == null) {
                return List.<String>of();
            }
            Map<String, String> hash = existing.asHash();
            // Real Redis's HGETALL reply is a flat array alternating
            // field, value, field, value, ... — not nested pairs — so we
            // build exactly that flat shape here, while still inside the
            // lock (iterating `hash` directly), then hand back a plain,
            // independent ArrayList copy.
            List<String> result = new ArrayList<>(hash.size() * 2);
            for (Map.Entry<String, String> entry : hash.entrySet()) {
                result.add(entry.getKey());
                result.add(entry.getValue());
            }
            return result;
        });

        return toBulkStringArray(flattened);
    }

    // ==================== SET commands ====================

    private CommandResult sadd(List<String> args) {
        if (args.size() < 2) {
            return wrongArgs("sadd");
        }
        String key = args.get(0);
        List<String> members = args.subList(1, args.size());

        int addedCount = dataStore.writeTransaction(map -> {
            RedisValue existing = map.get(key);
            Set<String> set;
            if (existing == null) {
                // LinkedHashSet, not plain HashSet: it keeps insertion
                // order, so SMEMBERS on a freshly-built set returns members
                // in a stable, predictable order (matching the order they
                // were SADD-ed) instead of HashMap's unspecified, hash-code
                // dependent iteration order. This isn't required by the
                // Redis protocol (real Redis doesn't guarantee set order
                // either), but it makes our own manual testing and the
                // Phase 8 dashboard far less confusing to look at, at
                // basically no extra cost.
                set = new LinkedHashSet<>();
                map.put(key, RedisValue.ofSet(set));
            } else {
                set = existing.asSet();
            }
            int added = 0;
            for (String member : members) {
                // Set.add returns true only if the item WASN'T already
                // present — exactly the "count of newly added members"
                // semantics real Redis's SADD reply has.
                if (set.add(member)) {
                    added++;
                }
            }
            return added;
        });

        return CommandResult.integer(addedCount);
    }

    private CommandResult srem(List<String> args) {
        if (args.size() < 2) {
            return wrongArgs("srem");
        }
        String key = args.get(0);
        List<String> members = args.subList(1, args.size());

        int removedCount = dataStore.writeTransaction(map -> {
            RedisValue existing = map.get(key);
            if (existing == null) {
                return 0;
            }
            Set<String> set = existing.asSet();
            int removed = 0;
            for (String member : members) {
                if (set.remove(member)) {
                    removed++;
                }
            }
            if (set.isEmpty()) {
                map.remove(key);
            }
            return removed;
        });

        return CommandResult.integer(removedCount);
    }

    private CommandResult smembers(List<String> args) {
        if (args.size() != 1) {
            return wrongArgs("smembers");
        }
        String key = args.get(0);
        List<String> members = dataStore.readTransaction(map -> {
            RedisValue existing = map.get(key);
            return existing == null ? List.<String>of() : new ArrayList<>(existing.asSet());
        });
        return toBulkStringArray(members);
    }

    private CommandResult sismember(List<String> args) {
        if (args.size() != 2) {
            return wrongArgs("sismember");
        }
        String key = args.get(0);
        String member = args.get(1);
        boolean isMember = dataStore.readTransaction(map -> {
            RedisValue existing = map.get(key);
            return existing != null && existing.asSet().contains(member);
        });
        return CommandResult.integer(isMember ? 1 : 0);
    }

    // ==================== Small shared helpers ====================

    private CommandResult wrongArgs(String commandName) {
        return CommandResult.error("ERR wrong number of arguments for '" + commandName + "' command");
    }

    private CommandResult toBulkStringArray(List<String> values) {
        List<CommandResult> items = new ArrayList<>(values.size());
        for (String value : values) {
            items.add(CommandResult.bulkString(value));
        }
        return CommandResult.array(items);
    }

    /**
     * CommandResult represents "what to reply", modeled directly on RESP's
     * own reply types (RESP itself is covered in full in Phase 3's
     * RESPParser.java — this is a deliberate preview so CommandProcessor
     * doesn't need to know anything about the wire format yet):
     *
     *   SIMPLE_STRING -> RESP "+OK\r\n" style replies (short status text,
     *                     never contains \r or \n itself)
     *   ERROR         -> RESP "-ERR message\r\n" style replies
     *   INTEGER       -> RESP ":123\r\n" style replies
     *   BULK_STRING   -> RESP "$3\r\nfoo\r\n" style replies — this is also
     *                     how a "nil" (missing key) reply is represented:
     *                     BULK_STRING with a null payload becomes RESP's
     *                     special "$-1\r\n" (nil bulk string)
     *   ARRAY         -> RESP "*N\r\n..." style replies — a list of OTHER
     *                     CommandResults (e.g. LRANGE's reply is an ARRAY
     *                     of BULK_STRINGs)
     *
     * This class is declared as a `static` nested class inside
     * CommandProcessor (the same pattern we used for
     * RedisValue.WrongTypeException) because, for now, it only exists to
     * describe CommandProcessor's output — nothing else in Phase 1 needs
     * it. Once Phase 3's ClientHandler needs to consume CommandResult to
     * write actual RESP bytes back to a socket, it will simply import
     * CommandProcessor.CommandResult; if that ever feels awkward we can
     * always promote it to its own top-level file later — nested classes
     * can be moved out without changing how any of the logic above works.
     */
    public static final class CommandResult {

        public enum Type { SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, ARRAY }

        private final Type type;
        private final Object payload;

        private CommandResult(Type type, Object payload) {
            this.type = type;
            this.payload = payload;
        }

        public static CommandResult simpleString(String value) {
            return new CommandResult(Type.SIMPLE_STRING, value);
        }

        public static CommandResult error(String message) {
            return new CommandResult(Type.ERROR, message);
        }

        public static CommandResult integer(long value) {
            return new CommandResult(Type.INTEGER, value);
        }

        /** Pass null for a "nil" bulk string reply (RESP's "$-1\r\n"). */
        public static CommandResult bulkString(String value) {
            return new CommandResult(Type.BULK_STRING, value);
        }

        public static CommandResult nilBulkString() {
            return bulkString(null);
        }

        /** Pass null for a "nil" array reply (RESP's "*-1\r\n"). */
        public static CommandResult array(List<CommandResult> items) {
            return new CommandResult(Type.ARRAY, items);
        }

        public Type getType() {
            return type;
        }

        /** Valid for SIMPLE_STRING, ERROR, and BULK_STRING (may be null for a nil bulk string). */
        public String getStringPayload() {
            return (String) payload;
        }

        /** Valid for INTEGER. */
        public long getIntegerPayload() {
            return (Long) payload;
        }

        /** Valid for ARRAY (may be null for a nil array). */
        @SuppressWarnings("unchecked")
        public List<CommandResult> getArrayPayload() {
            return (List<CommandResult>) payload;
        }
    }
}
