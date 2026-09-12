package com.redis.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * RaftLog — Phase 7's second file. A thread-safe, ordered list of
 * LogEntry objects, one per Raft node, using RAFT'S OWN 1-INDEXED
 * convention from the original Raft paper — NOT Java's usual 0-indexed
 * convention. Index 0 is a special sentinel meaning "before the very
 * first entry" (equivalently: "the log is empty"), which lets
 * lastIndex()==0 and termAt(0)==0 both cleanly mean "nothing here yet"
 * without any extra special-casing anywhere else in this project's Raft
 * code. Every PUBLIC method here takes/returns 1-indexed positions;
 * internally, we still store entries in a plain 0-indexed ArrayList (Java
 * gives us no other real option) and just subtract 1 wherever needed.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    /**
     * `synchronized` — a new concurrency keyword, and a DELIBERATELY
     * simpler tool than DataStore's ReentrantReadWriteLock (Phase 1).
     * Marking a method `synchronized` means only ONE thread can be
     * executing ANY synchronized method on this same object at a time —
     * every other thread calling any synchronized method on it simply
     * waits its turn. There's no separate lock object to create and no
     * explicit lock()/unlock() calls to remember: the JVM acquires this
     * object's built-in lock automatically the instant a synchronized
     * method starts, and releases it automatically the instant that
     * method returns — even if an exception is thrown partway through.
     * It's the exact same safety guarantee a manual try/finally around a
     * ReentrantLock gives, just built directly into the language with
     * less to get wrong.
     *
     * WHY THIS INSTEAD OF A ReentrantReadWriteLock LIKE DataStore USES:
     * DataStore specifically chose a read/write lock because GETs vastly
     * outnumber SETs in a typical workload, so letting many reads run
     * concurrently is a real, meaningful win there. RaftLog has no such
     * lopsided pattern — appending an entry, checking the last index, or
     * truncating on conflict are all quick, simple operations with
     * nothing to gain from allowing multiple simultaneous readers.
     * `synchronized` is the right, simplest tool when "one thread at a
     * time, plain and simple" is all you actually need — reaching for a
     * fancier lock without a concrete reason would just be unnecessary
     * complexity.
     */
    public synchronized int append(LogEntry entry) {
        entries.add(entry);
        return entries.size(); // this entry's new 1-indexed position
    }

    /**
     * Returns the entry at `index` (1-indexed). Throws
     * IndexOutOfBoundsException if it doesn't exist — Raft's own
     * algorithm (RaftNode.java, a later file) is always careful to only
     * ever ask for an index it has already confirmed exists, so hitting
     * this exception here would mean a genuine bug worth failing loudly
     * on, not something to quietly paper over.
     */
    public synchronized LogEntry get(int index) {
        return entries.get(index - 1);
    }

    /** The 1-indexed position of the last entry, or 0 if the log is empty. */
    public synchronized int lastIndex() {
        return entries.size();
    }

    /**
     * The term of the last entry, or 0 if the log is empty. 0 is a safe,
     * unambiguous sentinel here because real Raft terms always start
     * counting from 1 — a brand-new node that has never seen any entry
     * or any election correctly reports term 0, matching real Raft's own
     * convention for "nothing has happened here yet."
     */
    public synchronized long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    /**
     * The term stored at a given 1-indexed position, or 0 if that
     * position doesn't exist (the log is shorter than that, OR index 0
     * itself was asked for) — both cases correctly mean "there's no real
     * term to compare against here." This gets used constantly by
     * Raft's log-matching checks (inside AppendEntries handling, a later
     * file), which are written to treat a 0 result as "definitely does
     * not match," always correct since a genuine term is never less
     * than 1.
     */
    public synchronized long termAt(int index) {
        if (index <= 0 || index > entries.size()) {
            return 0;
        }
        return entries.get(index - 1).term();
    }

    /**
     * Deletes every entry from `fromIndex` (1-indexed, inclusive)
     * onward. Used when a follower discovers, while processing an
     * AppendEntries RPC, that its own log holds entries which CONFLICT
     * with what the current, legitimate leader says should be there —
     * typically leftover entries from an earlier leader that a majority
     * never actually confirmed, now correctly being overwritten by the
     * current leader's version of history. `subList(...).clear()`
     * removes exactly that trailing portion from the backing ArrayList
     * in place, without needing to rebuild the whole list by hand.
     */
    public synchronized void truncateFrom(int fromIndex) {
        if (fromIndex <= 0 || fromIndex > entries.size()) {
            return; // nothing in range to truncate
        }
        entries.subList(fromIndex - 1, entries.size()).clear();
    }

    /**
     * How many entries this log currently holds. Numerically identical
     * to lastIndex() — kept as its own method purely so call sites can
     * say whichever one actually matches what they're asking ("how big
     * is this log?" vs. "what position is the last entry at?"), even
     * though the answer is the same number either way.
     */
    public synchronized int size() {
        return entries.size();
    }
}
