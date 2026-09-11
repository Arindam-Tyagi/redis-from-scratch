package com.redis;

import com.redis.core.CommandProcessor;
import com.redis.core.DataStore;
import com.redis.network.Server;
import com.redis.persistence.SnapshotManager;
import com.redis.persistence.WriteAheadLog;
import com.redis.ttl.ExpiryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * Main — Phase 3's final file. This is the entry point: the one method the
 * JVM actually calls when you run `java -jar redis-server.jar ...` (recall
 * pom.xml's maven-shade-plugin baked "com.redis.Main" into the JAR's
 * manifest specifically so this works). Its job is pure WIRING — creating
 * every object this project has built so far, in the correct order, and
 * connecting them together — it contains no Redis logic of its own at all.
 *
 * ===== The startup sequence, and why THIS order matters =====
 * 1. Load the snapshot into a fresh DataStore (fast — it's already the
 *    final state as of whenever the last snapshot was taken).
 * 2. Replay whatever's left in the WAL on top of that. Because
 *    SnapshotManager.save() always truncates the WAL right after a
 *    successful snapshot (Phase 2), the WAL at this point can ONLY contain
 *    commands that happened AFTER that last snapshot — so replaying it on
 *    top of the loaded snapshot reconstructs the exact state right before
 *    whatever crash or shutdown happened, with no double-counting and no
 *    gaps.
 * 3. Only once DataStore correctly reflects everything that ever happened
 *    do we start accepting new client connections — accepting connections
 *    before recovery finished could let a client read stale/incomplete
 *    data or make new changes that get confused with recovery in progress.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            // System.err (not System.out) is the conventional stream for
            // error/usage messages — this lets a shell script or another
            // program distinguish normal output from something that
            // indicates a problem, even when both streams are being
            // watched at once.
            System.err.println("Usage: java -jar redis-server.jar <path-to-config.properties>");
            // A non-zero exit code (1 here) is the standard way a program
            // tells whatever launched it "something was wrong" — a calling
            // script can check this to decide whether to proceed, retry,
            // or alert someone. Exit code 0 conventionally means success.
            System.exit(1);
            return;
        }

        Properties config = loadConfig(args[0]);
        int port = Integer.parseInt(config.getProperty("port"));
        Path walPath = Path.of(config.getProperty("wal.path"));
        Path snapshotPath = Path.of(config.getProperty("snapshot.path"));

        DataStore dataStore = new DataStore();
        SnapshotManager snapshotManager = new SnapshotManager(snapshotPath);
        WriteAheadLog wal = new WriteAheadLog(walPath);
        CommandProcessor commandProcessor = new CommandProcessor(dataStore);

        logger.info("Loading snapshot from {}", snapshotPath);
        snapshotManager.loadInto(dataStore);
        logger.info("DataStore has {} key(s) after snapshot load", dataStore.size());

        logger.info("Replaying WAL from {}", walPath);
        List<List<String>> walCommands = wal.readAll();
        for (List<String> command : walCommands) {
            // We call commandProcessor.process(...) DIRECTLY here — NOT
            // through anything that would also call wal.append(...) again.
            // These commands are ALREADY durably sitting in the very WAL
            // file we just read them from; logging them into it a second
            // time during replay would just pointlessly duplicate every
            // entry on every single restart, making the WAL grow forever
            // for no reason. Replay only ever APPLIES to memory — it never
            // re-logs.
            commandProcessor.process(command);
        }
        logger.info("Replayed {} WAL command(s); DataStore now has {} key(s)",
                walCommands.size(), dataStore.size());

        // ===== Phase 4: start active expiry =====
        // Everything above this point is recovery — restoring exactly the
        // state the server had before it last stopped. Active expiry is
        // ongoing, ordinary server operation, so it starts fresh here, AFTER
        // recovery is done, exactly like the client-accepting Server below
        // it. Starting it any earlier (e.g. during WAL replay) would risk it
        // sweeping through a DataStore that isn't fully reconstructed yet.
        ExpiryManager expiryManager = new ExpiryManager(dataStore);
        expiryManager.start();

        // ===== Shutdown hook — a new JVM concept =====
        // Runtime.getRuntime().addShutdownHook(thread) registers a Thread
        // that the JVM will automatically start and wait for whenever the
        // process is shutting down NORMALLY — this covers the program
        // reaching a natural end, an uncaught exception propagating out of
        // main(), and (on most systems) Ctrl+C in the terminal, which sends
        // a request the JVM can intercept this way. It does NOT run on a
        // hard kill (like `kill -9`, or the process/power being cut) —
        // exactly the crash scenario our WAL exists to protect against in
        // the first place, so it's not a substitute for the durability
        // work we've already done, only a nice-to-have for the CLEAN
        // shutdown case: it lets us close the WAL's file handle properly
        // rather than relying on the OS to clean it up eventually.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down — stopping active expiry and closing WAL file handle...");
            expiryManager.stop();
            try {
                wal.close();
            } catch (IOException e) {
                logger.error("Error while closing WAL during shutdown", e);
            }
        }));

        Server server = new Server(port, commandProcessor, wal);
        logger.info("Starting server...");
        server.start(); // blocks forever — this call never normally returns
    }

    /**
     * java.util.Properties — a simple, built-in JDK class for reading
     * "key=value" formatted config files (exactly the format of our
     * config/node1.properties files). properties.load(inputStream) reads
     * every "key=value" line from the stream and populates the Properties
     * object (which behaves like a Map<String,String>) accordingly —
     * blank lines and lines starting with '#' are treated as comments and
     * ignored, which is why our config files below can include explanatory
     * comments directly.
     */
    private static Properties loadConfig(String path) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            properties.load(in);
        }
        return properties;
    }
}
