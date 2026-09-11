package com.redis.ttl;

import com.redis.core.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * ExpiryManager — Phase 4's actual new file. Implements ACTIVE expiry: a
 * background sweep that periodically finds and removes expired keys on its
 * own, even if no client ever reads them again. This is the second half of
 * "lazy + active expiry" from the project brief — the "lazy" half has
 * quietly existed since Phase 1, built right into DataStore.get() (which
 * removes a key the moment anyone tries to read it AFTER it's expired).
 *
 * ===== Why we need ACTIVE expiry at all, if lazy already works =====
 * Lazy expiry alone has a real gap: a key that expires and is NEVER read
 * again by anyone would simply sit in DataStore's map forever, quietly
 * consuming memory, with nothing ever triggering its removal. Imagine a
 * session token set with a 10-minute TTL that a user never checks again
 * after logging out — without active expiry, that dead key lives in RAM
 * indefinitely. Active expiry closes that gap by periodically walking
 * through keys on its own initiative and cleaning up anything it finds
 * that's expired, independent of whether any client ever asks about it
 * again.
 *
 * ===== ExecutorService / ScheduledExecutorService — a new concept =====
 * We need some code to run REPEATEDLY, forever, on a timer, in the
 * background, for the entire lifetime of the server. We could write this
 * ourselves with a raw Thread and a `while(true) { doWork(); sleep(...); }`
 * loop, but the JDK already provides a well-tested, purpose-built tool for
 * EXACTLY this: `ScheduledExecutorService`.
 *
 * An `ExecutorService` (the more general concept this specializes) is a
 * managed pool of worker threads that you submit TASKS to, instead of
 * manually creating and managing Thread objects yourself — you say "run
 * this piece of work" and the executor handles thread creation, reuse, and
 * lifecycle for you. `ScheduledExecutorService` extends that idea with
 * timing: `scheduleWithFixedDelay(task, initialDelay, delay, unit)` runs
 * `task` once after `initialDelay`, then keeps re-running it, always
 * waiting exactly `delay` after the PREVIOUS run FINISHED before starting
 * the next one. (This is different from `scheduleAtFixedRate`, its
 * sibling, which instead tries to start every run at a fixed clock
 * interval regardless of how long each run took — if a single run ever
 * took longer than the interval, fixed-rate could end up queuing up
 * back-to-back catch-up runs. For a sweep like ours, where we'd always
 * rather wait a calm, predictable gap between sweeps than risk them
 * piling up, fixed-DELAY is the more defensive, appropriate choice.)
 *
 * ===== Why a Set.of(...)-daemon PLATFORM thread here, not a virtual one =====
 * Server.java uses `Thread.ofVirtual()` for each client connection because
 * there can be THOUSANDS of those, each mostly sitting idle waiting for
 * network I/O — exactly the scenario virtual threads are built for. Here,
 * there is only ONE background sweep task, running occasionally, doing a
 * small amount of CPU work (scanning keys) rather than waiting on network
 * I/O. A single ordinary platform thread is perfectly appropriate and
 * simpler here — virtual threads solve a "we need many cheap threads"
 * problem that doesn't apply to this one single recurring task.
 *
 * We DO mark this thread as a "daemon" thread, though — another new
 * concept. Java distinguishes "user" threads from "daemon" threads: the
 * JVM will keep running as long as ANY non-daemon (user) thread is still
 * alive, but it will exit as soon as ONLY daemon threads remain, killing
 * them abruptly without waiting for them to finish whatever they're doing.
 * Our main server thread (running Server.start()'s accept loop) is a
 * regular non-daemon thread, so in normal operation the JVM never exits
 * anyway. But marking this background sweep thread as a daemon is still
 * the right, defensive choice: it guarantees this thread can NEVER, on its
 * own, keep the JVM alive past when everything else is done — useful
 * during testing too, where forgetting to call stop() on an ExpiryManager
 * in a test could otherwise leave a lingering thread that prevents the
 * test process from exiting cleanly.
 */
public class ExpiryManager {

    private static final Logger logger = LoggerFactory.getLogger(ExpiryManager.class);

    // How often the background sweep runs. Real Redis sweeps roughly 10
    // times per second (every 100ms); we match that here. Unlike real
    // Redis (which samples a small RANDOM subset of keys with an expiry
    // each cycle, for scalability on huge datasets — a fairly involved
    // probabilistic algorithm), we do a SIMPLE FULL SCAN of every key each
    // sweep. For a from-scratch learning project at realistic test/demo
    // scale, a full scan is simpler to reason about and completely
    // correct; it would only become a real performance concern with a
    // genuinely huge key count, which is a reasonable Phase 10 tuning
    // topic rather than something to over-engineer now.
    private static final long SWEEP_INTERVAL_MILLIS = 100;

    private final DataStore dataStore;
    private final ScheduledExecutorService scheduler;

    public ExpiryManager(DataStore dataStore) {
        this.dataStore = dataStore;

        // A ThreadFactory is exactly what it sounds like: an object whose
        // one job is to create Thread objects a certain way, whenever an
        // ExecutorService needs a new one. We supply a custom one here
        // purely to (a) mark the thread as a daemon (explained above) and
        // (b) give it a clear, recognizable name — "expiry-sweep-thread" —
        // which shows up in stack traces, thread dumps, and log output,
        // making it obvious at a glance which thread is doing what if we
        // ever need to debug this server while it's running.
        ThreadFactory daemonThreadFactory = runnable -> {
            Thread thread = new Thread(runnable, "expiry-sweep-thread");
            thread.setDaemon(true);
            return thread;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory);
    }

    /**
     * Begins the recurring background sweep. Safe to call once per
     * ExpiryManager instance (calling it twice would schedule the sweep
     * twice, running it redundantly — Main.java, where this gets wired in
     * next, will only ever call this once per server startup).
     */
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::sweepExpiredKeys,
                SWEEP_INTERVAL_MILLIS,
                SWEEP_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS
        );
        logger.info("Active expiry sweep started (every {}ms)", SWEEP_INTERVAL_MILLIS);
    }

    /**
     * Stops the background sweep and releases its thread. Should be called
     * during a clean shutdown (Main.java's shutdown hook, alongside closing
     * the WAL) so this thread doesn't linger unnecessarily — though since
     * it's a daemon thread, forgetting this would never actually prevent
     * the JVM from exiting, just leave the thread running pointlessly
     * until then.
     */
    public void stop() {
        scheduler.shutdown();
        logger.info("Active expiry sweep stopped");
    }

    /**
     * The actual sweep logic, run on our one dedicated background thread
     * every SWEEP_INTERVAL_MILLIS.
     *
     * THE KEY TRICK: we don't need to duplicate any "is this expired, and
     * if so remove it" logic here at all — DataStore.get(key) ALREADY does
     * exactly that (that's lazy expiry, built in Phase 1). So this sweep
     * simply calls get() on every currently-existing key; for any key that
     * turns out to be expired, get()'s own internal logic removes it right
     * then, as a side effect of just trying to read it. We're not
     * "actively expiring" using different code from lazy expiry — we're
     * using lazy expiry's own correctness, just triggered proactively by
     * us instead of waiting for a client to ask.
     *
     * dataStore.keys() returns a SNAPSHOT copy (see DataStore.java) — safe
     * to iterate here even while other threads are concurrently adding,
     * removing, or reading keys, since we're never iterating DataStore's
     * own live internal structure.
     */
    private void sweepExpiredKeys() {
        // A ScheduledExecutorService has an important, easy-to-miss
        // gotcha: if a scheduled task ever throws an uncaught exception,
        // that silently CANCELS ALL FUTURE RUNS of that task — with no
        // obvious error printed anywhere by default. A single unexpected
        // bug on one sweep would otherwise permanently and silently kill
        // active expiry for the rest of the server's uptime. Wrapping the
        // whole sweep in a try/catch that logs anything unexpected (rather
        // than letting it escape) is what prevents that from ever
        // happening here.
        try {
            Set<String> keys = dataStore.keys();
            int removedCount = 0;
            for (String key : keys) {
                boolean stillPresent = dataStore.get(key).isPresent();
                if (!stillPresent) {
                    // Since `key` came from a snapshot of keys that DID
                    // exist a moment ago, `get()` now returning empty means
                    // one of two things happened: either it just expired
                    // and get() lazily removed it (the case we care about),
                    // or another thread deleted/expired it independently in
                    // the tiny window since our snapshot was taken. Either
                    // way, counting it here is harmless — this count is
                    // purely for logging/observability, never used for any
                    // correctness decision.
                    removedCount++;
                }
            }
            if (removedCount > 0) {
                logger.debug("Active expiry sweep removed {} key(s)", removedCount);
            }
        } catch (Exception e) {
            logger.error("Unexpected error during active expiry sweep", e);
        }
    }
}
