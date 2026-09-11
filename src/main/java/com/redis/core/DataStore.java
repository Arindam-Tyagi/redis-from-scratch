package com.redis.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * DataStore is the actual in-memory key-value table: one Java Map from
 * String (the key) to RedisValue (the tagged-union wrapper we built in
 * Phase 1's first file).
 *
 * WHY THIS CLASS NEEDS MORE THAN "just a HashMap":
 * A plain java.util.HashMap is NOT thread-safe. Our server will eventually
 * handle many client connections at the same time (Phase 3 uses one virtual
 * thread per client), so multiple threads could call get()/set()/delete() on
 * this SAME DataStore instance at the exact same moment. With a plain
 * HashMap, two concurrent writes can literally corrupt its internal
 * structure (famously, a concurrent HashMap resize can even create an
 * infinite loop / hang the JVM). So we need to protect every read and write
 * with a lock. This file's whole purpose is: wrap a plain HashMap with
 * correct, efficient locking.
 */
public class DataStore {

    // The actual storage. Package-private-by-convention: nothing outside
    // this class ever touches `store` directly — every access goes through
    // a method below that first acquires the correct lock. This is the key
    // discipline for the whole class: if even ONE method reads or writes
    // `store` without holding a lock, all our thread-safety guarantees are
    // void, because the JVM/CPU give no protection by default.
    private final Map<String, RedisValue> store = new HashMap<>();

    /**
     * ReentrantReadWriteLock — first time this concept appears, so let's
     * build it up from scratch.
     *
     * THE PROBLEM WITH A SIMPLE LOCK (like `synchronized` or a plain Lock):
     * A simple mutex only allows ONE thread in at a time, period — whether
     * that thread wants to just READ a value or WRITE one. But in a
     * key-value store, GET commands are typically far more frequent than
     * SET/DEL commands, and multiple GETs happening at the same time are
     * perfectly safe — nobody is changing anything, so there's no way for
     * two simultaneous reads to conflict with each other. Forcing every GET
     * to wait in line behind every other GET would be a huge, unnecessary
     * bottleneck.
     *
     * WHAT ReentrantReadWriteLock GIVES US:
     * It's actually two locks bundled together, sharing one internal state:
     * - readLock(): any number of threads can hold the read lock AT THE
     * SAME TIME, as long as no thread holds the write lock. Many
     * concurrent GETs run in parallel with each other.
     * - writeLock(): completely exclusive. If any thread holds the write
     * lock, no other thread — reader or writer — can hold ANY lock at
     * the same time. This guarantees that while a SET/DEL is happening,
     * nobody else can see a half-updated Map.
     * So reads scale with concurrency, and writes remain fully safe.
     *
     * WHY "Reentrant":
     * "Reentrant" means the SAME thread can acquire a lock it already holds,
     * again, without deadlocking itself. Example: if method A holds the
     * write lock and, before releasing it, calls method B which ALSO tries
     * to acquire the write lock — with a reentrant lock, that succeeds
     * immediately (the JVM just increments an internal "hold count" for
     * that thread), instead of that same thread blocking forever waiting
     * for itself to release a lock it's still holding. We don't strictly
     * need this behavior yet in this file, but it's what makes
     * ReentrantReadWriteLock safe to use even as our codebase grows and
     * methods start calling each other.
     *
     * We declare the field as the `ReadWriteLock` INTERFACE type (not the
     * concrete `ReentrantReadWriteLock` class) by convention — "program to
     * an interface, not an implementation" — even though we only ever
     * instantiate ReentrantReadWriteLock. This means if some future Java
     * version offers a different ReadWriteLock implementation, we'd only
     * need to change the one line below, not every place `lock` is used.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Reads the value stored at `key`, if any.
     *
     * Returns Optional<RedisValue> instead of a plain RedisValue that might
     * be `null`. Concept: Optional is a container that either holds exactly
     * one value (Optional.of(value)) or holds nothing (Optional.empty()).
     * The point of using it here instead of returning `null` directly is
     * that it forces callers to explicitly handle the "key doesn't exist"
     * case (via isPresent()/isEmpty(), orElse(...), ifPresent(...), etc.)
     * rather than accidentally calling a method on a null reference and
     * blowing up with a NullPointerException somewhere far from the actual
     * bug. It's a way of making "this might not have a value" visible in
     * the method's signature itself, instead of a silent possibility you
     * have to remember.
     *
     * LAZY EXPIRY happens right here. This is the "lazy" half of the
     * "lazy + active expiry" requirement from the project brief — we don't
     * need a background thread to catch every expired key immediately;
     * instead, the very next time ANYONE tries to read an expired key, we
     * catch it right then and remove it. (The "active" half — a background
     * sweep that finds and removes expired keys even if nobody reads them —
     * is ExpiryManager's job in Phase 4.)
     *
     * NOTICE THE LOCK-UPGRADE PROBLEM this method has to work around:
     * ReentrantReadWriteLock does NOT allow you to upgrade a held read lock
     * directly into a write lock (trying to acquire the write lock while
     * still holding the read lock would deadlock, because the write lock
     * can't be granted while any read lock is outstanding — including your
     * own). So the pattern here is:
     * 1. Acquire the READ lock, just to check if the key is expired.
     * 2. If it's NOT expired, return it immediately and release the read
     * lock — this is the fast, common path.
     * 3. If it IS expired, release the read lock FIRST, then acquire the
     * WRITE lock, and re-check (this is called "double-checked
     * locking") before removing — because between releasing the read
     * lock and acquiring the write lock, another thread could have
     * already removed this same key (e.g. ExpiryManager's active sweep
     * running concurrently), so we must not assume our first check is
     * still valid.
     */
    public Optional<RedisValue> get(String key) {
        lock.readLock().lock();
        RedisValue value;
        try {
            value = store.get(key);
            if (value == null) {
                return Optional.empty();
            }
            if (!value.isExpired()) {
                return Optional.of(value);
            }
            // Falls through to the expired-key cleanup path below.
        } finally {
            // `finally` guarantees this unlock runs even if an exception is
            // thrown above, or even because of the early `return` statements
            // in the try block — Java runs `finally` blocks before a method
            // actually returns. Forgetting to unlock in a finally block is
            // one of the most common ways to introduce a permanent deadlock
            // (a lock that never gets released again), so EVERY lock/unlock
            // pair in this whole project will follow this exact
            // lock() -> try { ... } -> finally { unlock() } shape.
            lock.readLock().unlock();
        }

        // We only reach here if the key existed but was expired.
        // Acquire the exclusive write lock to actually remove it.
        lock.writeLock().lock();
        try {
            RedisValue current = store.get(key);
            // Double-check: re-verify it's STILL there and STILL expired,
            // because time has passed (however briefly) since our read-lock
            // check, and another thread might have already deleted or even
            // overwritten this key with a fresh SET in the meantime.
            if (current != null && current.isExpired()) {
                store.remove(key);
            }
            return Optional.empty();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Stores `value` under `key`, overwriting whatever was there before
     * (matching real Redis's SET semantics — SET always replaces, it never
     * merges with an existing value, even if the existing value was a
     * different type).
     */
    public void set(String key, RedisValue value) {
        lock.writeLock().lock();
        try {
            store.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes `key` entirely. Returns true if the key existed (and was
     * therefore actually removed), false if there was nothing to remove —
     * this lets CommandProcessor later reply with Redis's DEL semantics,
     * which returns the COUNT of keys actually deleted, not just "OK".
     */
    public boolean delete(String key) {
        lock.writeLock().lock();
        try {
            return store.remove(key) != null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks existence WITHOUT the side effect of removing an expired key.
     * We deliberately reuse get(key) here rather than duplicating logic —
     * get() already does the correct "expired keys don't count as existing"
     * check (and cleans them up as a bonus), so exists() just asks "did
     * get() find something?"
     */
    public boolean exists(String key) {
        return get(key).isPresent();
    }

    /**
     * Returns the number of keys currently stored, INCLUDING any that have
     * technically expired but haven't been cleaned up yet (either by a
     * get() call or by ExpiryManager's active sweep). This matches how real
     * Redis's DBSIZE behaves too — it's a raw count of map entries, not a
     * "logically alive" count. We're being explicit about this in the
     * comment because it's exactly the kind of subtle mismatch that causes
     * confusing bugs later (e.g. the dashboard in Phase 8 showing a key
     * count that doesn't match how many keys actually respond to GET).
     */
    public int size() {
        lock.readLock().lock();
        try {
            return store.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns a SNAPSHOT copy of all current key names — a brand new
     * HashSet, not a live view into `store`. This matters: if we returned
     * `store.keySet()` directly, that returned Set is backed by the live
     * map, so (a) it would only be safe to read while still holding the
     * lock, which we can't guarantee once this method returns and releases
     * the lock, and (b) any later modification to `store` (from another
     * thread, at any time) would be visible through that same Set,
     * potentially even throwing a ConcurrentModificationException if
     * something tried to iterate it while another thread mutated `store`.
     * Copying the keys into a fresh, independent Set while we hold the read
     * lock avoids both problems entirely, at the cost of that copy's memory
     * — a completely reasonable tradeoff for an operation like KEYS that
     * isn't meant to be called on every single hot-path request anyway.
     */
    public Set<String> keys() {
        lock.readLock().lock();
        try {
            return new HashSet<>(store.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * ===== WHY THESE TWO METHODS EXIST (readTransaction / writeTransaction) =====
     *
     * get()/set()/delete() above are perfectly safe for STRING values,
     * because a Java String is IMMUTABLE — once created, its contents can
     * never change, so handing a String out after releasing the lock is
     * completely safe; nobody can corrupt it later.
     *
     * But LIST, HASH, and SET values (coming in CommandProcessor.java, the
     * next file) wrap MUTABLE Java collections (a List, a Map, a Set)
     * inside a RedisValue. If CommandProcessor did something like:
     * dataStore.get("mylist").get().asList().add("x");
     * that `.add("x")` call would run completely OUTSIDE any lock — get()
     * had already released the lock before returning. Two client threads
     * running LPUSH on the same key at the same moment could then corrupt
     * that ArrayList/LinkedList's internal structure, exactly the same class
     * of bug we built this whole locking scheme to prevent for the outer Map.
     *
     * The fix is to never hand out a mutable collection to be used AFTER
     * the lock is released. Instead, these two methods accept a lambda
     * (a small anonymous function) and run it while STILL HOLDING the lock,
     * giving that lambda direct access to the underlying map for its
     * duration only. Every command that touches a LIST/HASH/SET's internal
     * collection — read OR write — will go through one of these two
     * methods, so the entire "fetch the collection, then read/mutate it" is
     * one atomic, lock-protected operation from start to finish.
     *
     * The generic type parameter <R> means "the caller decides what type
     * this returns" — LPUSH will return an Integer (new list length),
     * SMEMBERS will return a List<String> (copied-out members), etc. Java
     * infers <R> automatically from the lambda you pass in; you'll never
     * need to write `DataStore.<Integer>writeTransaction(...)` by hand.
     *
     * IMPORTANT DISCIPLINE for whoever calls these (CommandProcessor):
     * never let a reference to the `Map<String, RedisValue> store` object
     * itself, or to any collection pulled out of it, escape the lambda —
     * e.g. never assign it to a field or return it directly. Only primitive
     * values or freshly-copied collections should come back out as <R>.
     * That's exactly the same "snapshot copy" discipline keys() already
     * follows above, just generalized to any read.
     */
    public <R> R readTransaction(Function<Map<String, RedisValue>, R> action) {
        lock.readLock().lock();
        try {
            return action.apply(store);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Same idea as readTransaction, but under the exclusive write lock —
     * use this whenever the lambda will ADD, REMOVE, or MUTATE anything
     * (put a new key, mutate a List/Map/Set in place, remove a key that
     * became empty, etc.).
     */
    public <R> R writeTransaction(Function<Map<String, RedisValue>, R> action) {
        lock.writeLock().lock();
        try {
            return action.apply(store);
        } finally {
            lock.writeLock().unlock();
        }
    }
}