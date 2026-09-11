package com.redis.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RedisValue is the "envelope" that wraps whatever data is stored under a key.
 *
 * WHY WE NEED THIS CLASS AT ALL:
 * Real Redis lets one key hold different KINDS of data: a plain string, a list,
 * a hash (map), a set, etc. But our DataStore (next file) is going to be a
 * single
 * Map<String, RedisValue> — one Java Map that holds EVERY key, regardless of
 * what type of value it points to. Java's Map needs one consistent value type
 * for all its entries, so we can't directly store "sometimes a String,
 * sometimes
 * a List" in the same map. RedisValue solves this: it is always the same class,
 * but internally it remembers WHICH type of data it's actually holding, plus
 * the raw data itself, plus TTL (expiry) information.
 *
 * This is a common pattern called a "tagged union" or "variant type" — one
 * container, with a tag (the `type` field) telling you how to interpret the
 * payload (the `data` field).
 */
public class RedisValue {

    /**
     * RedisType is a Java `enum` — a fixed, named set of constants.
     * Concept explained (first time it appears in this project):
     * An enum is Java's way of saying "this variable can only ever be one of
     * these exact values, nothing else." Unlike a plain String or int, the
     * compiler checks this for you — you cannot accidentally write
     * RedisType.STRING as "sting" (typo) and have it silently compile; a typo
     * would simply not exist as a valid enum constant, so the code wouldn't
     * compile. This is exactly the kind of type-safety we want for something
     * as fundamental as "what kind of value is this."
     *
     * We start with the 4 core Redis data types. We are NOT implementing
     * sorted sets or streams in this project — keeping scope realistic.
     */
    public enum RedisType {
        STRING,
        LIST,
        HASH,
        SET
    }

    // The tag: tells us how to interpret `data` below.
    // `final` means this field is assigned exactly once (in the constructor)
    // and can never be reassigned afterward. A RedisValue's TYPE never changes
    // after creation — if you want a different type at the same key, Redis
    // (and our CommandProcessor) will delete the old RedisValue and create a
    // brand new one, rather than mutating the type of an existing one.
    private final RedisType type;

    // The actual payload. Its declared type is `Object` (the root class every
    // Java class inherits from) because at compile time we don't know whether
    // it holds a String, a List<String>, a Map<String,String>, or a
    // Set<String> — we only find out at runtime by checking `type` first,
    // then safely casting. The type-safe accessor methods below (asString(),
    // asList(), etc.) are the ONLY way calling code should ever read `data` —
    // they check `type` before casting, so a caller can never accidentally
    // read a LIST value as if it were a STRING and get garbage or a confusing
    // ClassCastException from some random line of code.
    private final Object data;

    /**
     * Expiry timestamp, stored as "milliseconds since the Unix epoch"
     * (the same unit System.currentTimeMillis() returns). This is the exact
     * moment in wall-clock time after which this key should be treated as
     * gone, even though it's still physically sitting in memory until
     * ExpiryManager (Phase 4) actually removes it — that's what "lazy expiry"
     * means: we don't delete it the instant it expires, we just mark the
     * deadline, and check it lazily whenever the key is read.
     *
     * We use the sentinel value -1 to mean "no expiry set" (a key that lives
     * forever unless explicitly deleted), instead of using `null` with a
     * wrapper type like `Long`. Using a primitive `long` with a sentinel is
     * slightly more memory-efficient (no extra object on the heap for the
     * boxed Long) and avoids NullPointerException risk entirely — there is
     * no "empty" state to null-check, just compare against -1.
     *
     * `volatile` keyword (first time it appears — full detail on WHY threads
     * matter comes in Server.java/ClientHandler.java in Phase 3, but the
     * short version): our Redis server will handle many client connections
     * concurrently, each potentially on its own thread. If Thread A calls
     * EXPIRE on this key (writing a new expireAt) while Thread B is reading
     * this key at almost the same instant, without `volatile` Thread B might
     * see a stale, cached copy of expireAt from before Thread A's write —
     * because each CPU core can cache values in its own local cache line.
     * `volatile` tells the JVM: "never let a thread cache this field locally;
     * always read/write it straight from main memory." That guarantees any
     * thread that reads expireAt after another thread wrote it will see the
     * up-to-date value. It does NOT make compound operations (like "read
     * then write based on what you read") atomic/thread-safe on its own —
     * for that we'll use locks (ReentrantReadWriteLock) in DataStore. But for
     * a single field where one thread writes a new value and other threads
     * just need to see it, `volatile` alone is the right, lightweight tool.
     */
    private volatile long expireAt = -1L;

    /**
     * The constructor is `private`. This is a deliberate design choice: we do
     * NOT want calling code to write `new RedisValue(RedisType.STRING, "hi")`
     * directly, because nothing would stop someone from passing
     * `new RedisValue(RedisType.STRING, someList)` — a STRING type tagged
     * with a List payload, which is a lie that will blow up later when
     * something calls asString() and gets a ClassCastException far away from
     * where the mistake was actually made.
     *
     * Instead we expose static "factory methods" below (ofString, ofList,
     * ofHash, ofSet) — each one only accepts the exact matching data type in
     * its own method signature. This makes an invalid combination of
     * type + data literally impossible to construct from outside this class.
     * This pattern is called the "static factory method" pattern.
     */
    private RedisValue(RedisType type, Object data) {
        this.type = type;
        this.data = data;
    }

    // ---------- Factory methods: the only way to create a RedisValue ----------

    public static RedisValue ofString(String value) {
        return new RedisValue(RedisType.STRING, value);
    }

    public static RedisValue ofList(List<String> value) {
        return new RedisValue(RedisType.LIST, value);
    }

    public static RedisValue ofHash(Map<String, String> value) {
        return new RedisValue(RedisType.HASH, value);
    }

    public static RedisValue ofSet(Set<String> value) {
        return new RedisValue(RedisType.SET, value);
    }

    // ---------- Type introspection ----------

    public RedisType getType() {
        return type;
    }

    // ---------- Type-safe accessors ----------
    // Each of these checks `type` first and throws a clear, specific
    // exception if the caller is asking for the wrong shape of data. This
    // mirrors real Redis's behaviour: if you run LPUSH on a key that holds a
    // String, real Redis replies with an error
    // "WRONGTYPE Operation against a key holding the wrong kind of value"
    // instead of corrupting data or crashing unpredictably. Our
    // WrongTypeException (defined at the bottom of this file) is that same
    // idea, and CommandProcessor (Phase 1, next file) will catch it and turn
    // it into that exact RESP error message sent back to the client.

    @SuppressWarnings("unchecked")
    public String asString() {
        if (type != RedisType.STRING) {
            throw new WrongTypeException(RedisType.STRING, type);
        }
        return (String) data;
    }

    @SuppressWarnings("unchecked")
    public List<String> asList() {
        if (type != RedisType.LIST) {
            throw new WrongTypeException(RedisType.LIST, type);
        }
        return (List<String>) data;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> asHash() {
        if (type != RedisType.HASH) {
            throw new WrongTypeException(RedisType.HASH, type);
        }
        return (Map<String, String>) data;
    }

    @SuppressWarnings("unchecked")
    public Set<String> asSet() {
        if (type != RedisType.SET) {
            throw new WrongTypeException(RedisType.SET, type);
        }
        return (Set<String>) data;
    }

    // ---------- TTL / expiry handling ----------
    // NOTE: These methods only manage the `expireAt` timestamp on THIS
    // object. They do not, by themselves, remove the key from DataStore.
    // Actually removing expired keys (both "lazily" when read, and
    // "actively" via a background sweep) is ExpiryManager's job in Phase 4.
    // RedisValue's only responsibility here is to correctly answer the
    // question "am I expired right now?" — a single, well-defined, testable
    // piece of logic that everything else (DataStore, ExpiryManager) can
    // rely on without duplicating the comparison logic in multiple places.

    /**
     * Sets an absolute expiry time. `ttlMillis` is a DURATION from now
     * (e.g. "expire in 5000 ms"), which we convert to an absolute deadline
     * by adding it to the current wall-clock time. We store the absolute
     * deadline (not the duration) because durations go stale the instant
     * time passes — storing "expires in 5 seconds" would require us to also
     * remember exactly when that 5-second countdown started. Storing the
     * absolute instant "expires at 1699999999000" needs no such bookkeeping;
     * any thread, at any later point, can just compare it to
     * System.currentTimeMillis().
     */
    public void setExpireInMillis(long ttlMillis) {
        this.expireAt = System.currentTimeMillis() + ttlMillis;
    }

    /**
     * Sets an already-absolute expiry timestamp directly. This overload
     * exists for two callers who already have an absolute time rather than
     * a duration: (1) the WAL replay logic in Phase 2, which reads back a
     * previously-computed absolute expireAt from disk and must restore the
     * exact same deadline, not compute a new one relative to "now"; and
     * (2) commands like EXPIREAT in real Redis that take an absolute Unix
     * timestamp directly from the client instead of a relative duration.
     */
    public void setExpireAtTimestamp(long absoluteEpochMillis) {
        this.expireAt = absoluteEpochMillis;
    }

    /** Removes any expiry — the key will live forever unless deleted. */
    public void removeExpiry() {
        this.expireAt = -1L;
    }

    public boolean hasExpiry() {
        return expireAt != -1L;
    }

    /**
     * Raw absolute expiry timestamp, or -1 if none is set. Used by
     * ExpiryManager to sort/prioritize keys by how soon they'll expire, and
     * by the dashboard (Phase 8) to show "keys expiring soon".
     */
    public long getExpireAt() {
        return expireAt;
    }

    /**
     * The single source of truth for "is this value expired right now?"
     * A key with no expiry (-1) is never expired. Otherwise, it's expired
     * the instant the current wall-clock time passes the stored deadline.
     */
    public boolean isExpired() {
        return hasExpiry() && System.currentTimeMillis() >= expireAt;
    }

    /**
     * WrongTypeException is defined as a "nested static class" here (a class
     * declared inside another class, with the `static` keyword) rather than
     * its own top-level file. We're keeping it here for now, early in the
     * project, specifically because it is small, only ever thrown by
     * RedisValue's own accessor methods, and conceptually belongs to
     * RedisValue's contract. `extends RuntimeException` makes it an
     * "unchecked" exception — meaning calling code is not FORCED by the
     * compiler to wrap every asString()/asList() call in a try-catch; it can
     * propagate up naturally to wherever we DO want to handle it (in
     * CommandProcessor, Phase 1's next file), which keeps intermediate code
     * clean.
     */
    public static class WrongTypeException extends RuntimeException {
        public WrongTypeException(RedisType expected, RedisType actual) {
            super("WRONGTYPE Operation against a key holding the wrong kind of value "
                    + "(expected " + expected + ", but key holds " + actual + ")");
        }
    }
}