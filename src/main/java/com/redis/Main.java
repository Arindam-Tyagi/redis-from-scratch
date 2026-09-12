package com.redis;

import com.redis.cluster.ClusterConfig;
import com.redis.core.CommandProcessor;
import com.redis.core.DataStore;
import com.redis.network.Server;
import com.redis.persistence.SnapshotManager;
import com.redis.persistence.WriteAheadLog;
import com.redis.replication.ReplicaClient;
import com.redis.replication.ReplicationManager;
import com.redis.ttl.ExpiryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
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

        // ===== Phase 6: primary or replica? =====
        // `role` defaults to "primary" when absent — this keeps every
        // config file from before Phase 6 (which never set `role` at all)
        // working exactly as before, unchanged.
        String role = config.getProperty("role", "primary").toLowerCase(Locale.ROOT);
        boolean isReplica = role.equals("replica");

        DataStore dataStore = new DataStore();
        SnapshotManager snapshotManager = new SnapshotManager(snapshotPath);
        WriteAheadLog wal = new WriteAheadLog(walPath);

        // ===== Phase 5: cluster topology (PRIMARY nodes only) =====
        // A replica doesn't participate in slot-ownership/MOVED routing
        // at all — it only ever receives commands its primary forwards to
        // it for keys that primary has ALREADY confirmed it owns, so
        // there's nothing for a replica to check. We simply never build a
        // ClusterConfig for a replica, and pass `null` into
        // CommandProcessor's cluster-aware constructor slot instead —
        // exactly the same "null means off" pattern used everywhere else
        // this project needs an optional feature (see CommandProcessor's
        // own ClusterConfig field, or Server's ReplicationManager field).
        CommandProcessor commandProcessor;
        if (isReplica) {
            commandProcessor = new CommandProcessor(dataStore);
            logger.info("Starting as a REPLICA node (role=replica)");
        } else {
            ClusterConfig clusterConfig = ClusterConfig.fromProperties(config);
            ClusterConfig.NodeInfo self = clusterConfig.self();
            logger.info("Cluster node '{}' owns slots {}-{} ({}:{})",
                    self.id(), self.slotStart(), self.slotEnd(), self.host(), self.port());
            commandProcessor = new CommandProcessor(dataStore, clusterConfig);
        }

        // ===== Recovery — SKIPPED for a replica, and here's exactly why =====
        // A primary recovers from its OWN local snapshot + WAL because
        // that IS the authoritative record of its data. A replica is
        // different: the moment it connects to its primary (via
        // ReplicaClient, started further below), it receives a FULL SYNC
        // — a fresh, complete, authoritative copy of whatever the primary
        // currently has. If we ALSO loaded this replica's own possibly-
        // stale local snapshot/WAL first, we'd risk ending up with EXTRA
        // keys that the full sync never mentions (because our full sync
        // only ever ADDS/UPDATES keys via SET/RPUSH/etc. — it doesn't
        // explicitly clear anything first) — e.g. a key that existed
        // during this replica's last run, got DELeted on the primary
        // since then, and would otherwise wrongly "survive" here forever.
        // Simplest correct fix: a replica always starts from a genuinely
        // empty DataStore and treats its primary's full sync as the one
        // and only source of truth, every single time it (re)connects.
        // (A real production system would instead try to reuse local data
        // and only fetch the DIFFERENCE since last connected, for speed —
        // a reasonable Phase 10 optimization, not needed at this scale.)
        if (isReplica) {
            logger.info("Replica node - skipping local snapshot/WAL recovery; "
                    + "will bootstrap fresh from a full sync with its primary instead");
        } else {
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
        }

        // ===== Phase 4: start active expiry =====
        // Everything above this point is recovery — restoring exactly the
        // state the server had before it last stopped. Active expiry is
        // ongoing, ordinary server operation, so it starts fresh here, AFTER
        // recovery is done, exactly like the client-accepting Server below
        // it. Starting it any earlier (e.g. during WAL replay) would risk it
        // sweeping through a DataStore that isn't fully reconstructed yet.
        ExpiryManager expiryManager = new ExpiryManager(dataStore);
        expiryManager.start();

        // ===== Phase 6: replication set-up =====
        // Exactly one of these two branches runs, matching this node's role:
        //   - A PRIMARY gets a ReplicationManager, handed to Server below so
        //     it can stream every future write out to any replica that
        //     connects (see Server.java/ClientHandler.java).
        //   - A REPLICA gets a ReplicaClient instead, started on its OWN
        //     dedicated background thread (NOT the main thread — the main
        //     thread is about to block forever inside server.start() below,
        //     so replication has to run independently of that). A replica's
        //     own Server (started the same as any other node's) still runs
        //     too, in case anything ever needs to connect to it directly —
        //     it just gets a `null` ReplicationManager, since a replica
        //     doesn't support further sub-replicas of its own in this
        //     project.
        ReplicationManager replicationManager = null;
        if (isReplica) {
            String masterHost = config.getProperty("master.host");
            int masterPort = Integer.parseInt(config.getProperty("master.port"));
            ReplicaClient replicaClient = new ReplicaClient(masterHost, masterPort, commandProcessor, wal);
            Thread replicaThread = new Thread(replicaClient, "replica-client-thread");
            replicaThread.start();
            logger.info("Started replica-client thread targeting primary at {}:{}", masterHost, masterPort);
        } else {
            replicationManager = new ReplicationManager();
        }

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

        Server server = new Server(port, commandProcessor, wal, dataStore, replicationManager);
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
