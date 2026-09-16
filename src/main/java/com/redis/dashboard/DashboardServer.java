package com.redis.dashboard;

import com.redis.cluster.ClusterConfig;
import com.redis.core.CommandProcessor;
import com.redis.core.DataStore;
import com.redis.core.RedisValue;
import com.redis.network.RESPParser;
import com.redis.persistence.WriteAheadLog;
import com.redis.raft.RaftNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * DashboardServer — Phase 8's HTTP-facing backend. Exposes THIS one node's
 * real internal state as JSON over plain HTTP (keys, Raft internals,
 * persistence stats, a real command relay, and a real admin kill-switch),
 * plus serves the dashboard's own static HTML page.
 *
 * ===== com.sun.net.httpserver.HttpServer — a brand-new networking concept =====
 * Every server this project has built so far (Server.java, Phase 3) speaks
 * raw RESP over a plain TCP socket: WE decide the entire wire format
 * ourselves, byte by byte. HTTP is a completely different, much more
 * widely-used protocol — the same one a real web browser already knows how
 * to speak. Rather than hand-rolling an HTTP parser too, the JDK ships a
 * small, built-in HTTP server in `com.sun.net.httpserver` (the `com.sun.*`
 * prefix signals it's not part of the officially-portable Java API, but
 * it's been bundled with every mainstream JDK for years and is more than
 * good enough for a small internal dashboard like this one).
 *
 * Instead of writing our own accept()-loop, we register HANDLERS against
 * specific URL PATHS, and HttpServer internally manages accepting
 * connections and calling the right handler for each one — "the framework
 * calls your code" rather than "your code calls the framework."
 *
 * ===== A deliberate simplification: query params, not REST path segments =====
 * A "real" REST API would expose one key's detail as GET /keys/{key}. This
 * project's HttpServer usage matches contexts by a fixed PREFIX only — it
 * has no built-in path-pattern router (no {key} placeholder support), and
 * writing one just for this would be real complexity for no real benefit
 * at this project's scale. So every endpoint that needs a parameter uses
 * an ordinary query string instead: GET /key?key=user:1001. Functionally
 * identical, just simpler to implement correctly.
 */
public class DashboardServer {

    private static final Logger logger = LoggerFactory.getLogger(DashboardServer.class);

    // A purely ILLUSTRATIVE reference figure for a typical disk-based
    // database's read latency - NOT measured from any real database this
    // project talks to (it doesn't talk to one at all). Shown on the
    // dashboard clearly labeled as illustrative, purely for scale/context
    // next to the REAL measured Redis latency - never presented as if it
    // were also a live measurement.
    private static final double ILLUSTRATIVE_DB_LATENCY_MS = 25.0;

    private final int dashboardPort;
    private final int redisPort; // this node's own client-facing Redis port
    private final RaftNode raftNode; // nullable - same "null means off" pattern used throughout this project
    private final ClusterConfig clusterConfig; // nullable
    private final DataStore dataStore;
    private final CommandProcessor commandProcessor;
    private final WriteAheadLog wal;
    private final Path snapshotPath;
    private final Path dashboardHtmlPath;

    private HttpServer httpServer;

    // ===== A persistent connection back to this node's OWN Redis port =====
    // Reused across many /status latency checks rather than opening a
    // fresh Socket every time, so what we measure is close to "how long
    // did the PING command itself take" rather than being dominated by
    // repeated TCP handshake overhead. Lazily created, transparently
    // reconnected if it ever breaks.
    private Socket pingSocket;
    private OutputStream pingOut;
    private InputStream pingIn;

    // For computing a REAL commands/sec rate: two samples of
    // CommandProcessor's running total, one second apart (see
    // buildStatusJson's own comment for the exact math).
    private long lastCommandCountSample = 0;
    private long lastCommandCountSampleAtMillis = System.currentTimeMillis();

    public DashboardServer(int dashboardPort, int redisPort, RaftNode raftNode, ClusterConfig clusterConfig,
                            DataStore dataStore, CommandProcessor commandProcessor, WriteAheadLog wal,
                            Path snapshotPath, Path dashboardHtmlPath) {
        this.dashboardPort = dashboardPort;
        this.redisPort = redisPort;
        this.raftNode = raftNode;
        this.clusterConfig = clusterConfig;
        this.dataStore = dataStore;
        this.commandProcessor = commandProcessor;
        this.wal = wal;
        this.snapshotPath = snapshotPath;
        this.dashboardHtmlPath = dashboardHtmlPath;
    }

    /**
     * Starts listening. Unlike Server.java's start() (which BLOCKS the
     * calling thread forever inside its own accept loop), HttpServer's own
     * accept loop already runs on ITS OWN internally-managed thread(s)
     * once start() is called - so this method returns almost immediately.
     */
    public void start() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(dashboardPort), 0);
        httpServer.createContext("/status", cors(new StatusHandler()));
        httpServer.createContext("/keys", cors(new KeysHandler()));
        httpServer.createContext("/key", cors(new KeyDetailHandler()));
        httpServer.createContext("/slot", cors(new SlotHandler()));
        httpServer.createContext("/raft", cors(new RaftHandler()));
        httpServer.createContext("/persistence", cors(new PersistenceHandler()));
        httpServer.createContext("/command", cors(new CommandHandler()));
        httpServer.createContext("/admin/kill", cors(new AdminKillHandler()));
        httpServer.createContext("/", new StaticFileHandler());
        httpServer.setExecutor(null); // null = HttpServer's own tiny default executor
        httpServer.start();
        logger.info("Dashboard HTTP server listening on port {}", dashboardPort);
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
        closePingConnection();
    }

    // ==================== Small shared helpers ====================

    /**
     * Wraps any handler with the CORS header every JSON endpoint needs:
     * the dashboard's HTML page is loaded from ONE node's port but needs
     * to fetch() all 9 nodes' DIFFERENT ports. Browsers block that
     * cross-port access by default unless the server being fetched from
     * explicitly opts in via this header - "*" is fine for a read-only,
     * non-sensitive internal endpoint like these.
     */
    private HttpHandler cors(HttpHandler inner) {
        return exchange -> {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            inner.handle(exchange);
        };
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /** Escapes a string so it's safe to embed inside our hand-rolled JSON - real key/value data can contain quotes, backslashes, or newlines. */
    private static String jsonEscape(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(value.length());
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> escaped.append(c);
            }
        }
        return escaped.toString();
    }

    private static String jsonString(String value) {
        return value == null ? "null" : "\"" + jsonEscape(value) + "\"";
    }

    /** Pulls a single query-string parameter (e.g. "key" out of "?key=user:1001"), URL-decoded. Returns null if absent. */
    private static String queryParam(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) continue;
            String key = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            if (key.equals(name)) {
                return URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    // ==================== GET /status ====================

    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            sendJson(exchange, buildStatusJson());
        }
    }

    private String buildStatusJson() {
        String selfId = raftNode != null ? raftNode.getSelfId() : "unknown";
        String role = raftNode != null ? raftNode.getRole().name() : "NONE";
        long term = raftNode != null ? raftNode.getCurrentTerm() : 0;
        String leaderId = raftNode != null ? raftNode.getLeaderId() : null;
        int slotStart = clusterConfig != null ? clusterConfig.self().slotStart() : -1;
        int slotEnd = clusterConfig != null ? clusterConfig.self().slotEnd() : -1;
        int keyCount = dataStore.size();
        long redisLatencyMicros = measureRedisLatencyMicros();

        int commitIndex = raftNode != null ? raftNode.getCommitIndex() : -1;
        int lastApplied = raftNode != null ? raftNode.getLastApplied() : -1;
        int logSize = raftNode != null ? raftNode.getLogSize() : -1;

        // Real JVM memory usage - free from Runtime, no new instrumentation needed.
        Runtime runtime = Runtime.getRuntime();
        long usedMemoryBytes = runtime.totalMemory() - runtime.freeMemory();

        // Real commands/sec: sample CommandProcessor's running total now,
        // compare against the LAST time we sampled it, divide by the
        // actual elapsed time between the two samples (not assumed to be
        // exactly 1.000s, since HTTP requests don't arrive on a perfectly
        // metronomic schedule).
        long now = System.currentTimeMillis();
        long currentCount = commandProcessor.getCommandCount();
        double elapsedSeconds = Math.max(0.001, (now - lastCommandCountSampleAtMillis) / 1000.0);
        double commandsPerSecond = (currentCount - lastCommandCountSample) / elapsedSeconds;
        lastCommandCountSample = currentCount;
        lastCommandCountSampleAtMillis = now;

        return "{"
                + "\"id\":" + jsonString(selfId) + ","
                + "\"role\":" + jsonString(role) + ","
                + "\"term\":" + term + ","
                + "\"leaderId\":" + jsonString(leaderId) + ","
                + "\"slotStart\":" + slotStart + ","
                + "\"slotEnd\":" + slotEnd + ","
                + "\"keyCount\":" + keyCount + ","
                + "\"redisLatencyMicros\":" + redisLatencyMicros + ","
                + "\"illustrativeDbLatencyMs\":" + ILLUSTRATIVE_DB_LATENCY_MS + ","
                + "\"commitIndex\":" + commitIndex + ","
                + "\"lastApplied\":" + lastApplied + ","
                + "\"logSize\":" + logSize + ","
                + "\"usedMemoryBytes\":" + usedMemoryBytes + ","
                + "\"commandsProcessedTotal\":" + currentCount + ","
                + "\"commandsPerSecond\":" + String.format("%.2f", commandsPerSecond)
                + "}";
    }

    /**
     * Sends a PING to THIS node's own client-facing Redis port over a
     * persistent socket and times the full round trip in microseconds.
     * PING specifically because it's the one command CommandProcessor
     * never subjects to cluster-ownership/MOVED checks and never touches
     * DataStore - so this measures pure command-handling + network
     * round-trip overhead, nothing else.
     */
    private synchronized long measureRedisLatencyMicros() {
        try {
            ensurePingConnection();
            long startNanos = System.nanoTime();
            pingOut.write("PING\r\n".getBytes(StandardCharsets.UTF_8));
            pingOut.flush();
            readLine(pingIn);
            return (System.nanoTime() - startNanos) / 1000;
        } catch (IOException e) {
            logger.debug("Latency self-check PING failed: {}", e.getMessage());
            closePingConnection();
            return -1;
        }
    }

    private void ensurePingConnection() throws IOException {
        if (pingSocket != null && pingSocket.isConnected() && !pingSocket.isClosed()) {
            return;
        }
        pingSocket = new Socket("localhost", redisPort);
        pingOut = pingSocket.getOutputStream();
        pingIn = pingSocket.getInputStream();
    }

    private void closePingConnection() {
        try {
            if (pingSocket != null) {
                pingSocket.close();
            }
        } catch (IOException ignored) {
            // Nothing meaningful to do - we're opening a fresh one next time regardless.
        }
        pingSocket = null;
    }

    // ==================== GET /keys ====================

    /**
     * Lists every key currently in this node's DataStore, with its type
     * and remaining TTL - a summary view, not full values (see
     * KeyDetailHandler for that). Reuses DataStore.keys() and .get(key)
     * exactly as any other part of this project would.
     */
    private class KeysHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Set<String> keys = dataStore.keys();
            StringBuilder json = new StringBuilder("[");
            boolean first = true;
            for (String key : keys) {
                Optional<RedisValue> value = dataStore.get(key);
                if (value.isEmpty()) continue; // lazily expired between keys() and get()
                if (!first) json.append(",");
                first = false;
                RedisValue redisValue = value.get();
                long ttlSeconds = redisValue.hasExpiry()
                        ? Math.max(0, (redisValue.getExpireAt() - System.currentTimeMillis() + 999) / 1000)
                        : -1;
                json.append("{")
                        .append("\"key\":").append(jsonString(key)).append(",")
                        .append("\"type\":").append(jsonString(redisValue.getType().name())).append(",")
                        .append("\"ttlSeconds\":").append(ttlSeconds)
                        .append("}");
            }
            json.append("]");
            sendJson(exchange, json.toString());
        }
    }

    // ==================== GET /key?key=... ====================

    private class KeyDetailHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String key = queryParam(exchange, "key");
            if (key == null) {
                sendJson(exchange, "{\"error\":\"missing 'key' query parameter\"}");
                return;
            }
            Optional<RedisValue> maybeValue = dataStore.get(key);
            if (maybeValue.isEmpty()) {
                sendJson(exchange, "{\"exists\":false,\"key\":" + jsonString(key) + "}");
                return;
            }
            RedisValue redisValue = maybeValue.get();
            String valueJson = switch (redisValue.getType()) {
                case STRING -> jsonString(redisValue.asString());
                case LIST -> jsonStringArray(redisValue.asList());
                case HASH -> jsonStringMap(redisValue.asHash());
                case SET -> jsonStringArray(new ArrayList<>(redisValue.asSet()));
            };
            long ttlSeconds = redisValue.hasExpiry()
                    ? Math.max(0, (redisValue.getExpireAt() - System.currentTimeMillis() + 999) / 1000)
                    : -1;
            int slot = clusterConfig != null ? clusterConfig.computeSlot(key) : -1;

            sendJson(exchange, "{"
                    + "\"exists\":true,"
                    + "\"key\":" + jsonString(key) + ","
                    + "\"type\":" + jsonString(redisValue.getType().name()) + ","
                    + "\"value\":" + valueJson + ","
                    + "\"ttlSeconds\":" + ttlSeconds + ","
                    + "\"slot\":" + slot
                    + "}");
        }
    }

    private static String jsonStringArray(List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(jsonString(values.get(i)));
        }
        return sb.append("]").toString();
    }

    private static String jsonStringMap(Map<String, String> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append(jsonString(entry.getKey())).append(":").append(jsonString(entry.getValue()));
        }
        return sb.append("}").toString();
    }

    // ==================== GET /slot?key=... ====================

    /**
     * Returns the REAL computed hash slot for any key, using this node's
     * actual ClusterConfig.computeSlot(key) - not a JavaScript
     * reimplementation guessing at Java's String.hashCode() algorithm,
     * which would be a real accuracy risk for the dashboard's slot
     * visualizer.
     */
    private class SlotHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String key = queryParam(exchange, "key");
            if (key == null || clusterConfig == null) {
                sendJson(exchange, "{\"available\":false}");
                return;
            }
            int slot = clusterConfig.computeSlot(key);
            ClusterConfig.NodeInfo owner = clusterConfig.ownerOf(slot);
            sendJson(exchange, "{"
                    + "\"available\":true,"
                    + "\"key\":" + jsonString(key) + ","
                    + "\"slot\":" + slot + ","
                    + "\"ownerId\":" + jsonString(owner.id()) + ","
                    + "\"ownerSlotStart\":" + owner.slotStart() + ","
                    + "\"ownerSlotEnd\":" + owner.slotEnd()
                    + "}");
        }
    }

    // ==================== GET /raft ====================

    private class RaftHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (raftNode == null) {
                sendJson(exchange, "{\"available\":false}");
                return;
            }
            StringBuilder events = new StringBuilder("[");
            List<RaftNode.RaftEvent> recent = raftNode.getRecentEvents();
            for (int i = 0; i < recent.size(); i++) {
                if (i > 0) events.append(",");
                RaftNode.RaftEvent event = recent.get(i);
                events.append("{\"timestampMillis\":").append(event.timestampMillis())
                        .append(",\"message\":").append(jsonString(event.message())).append("}");
            }
            events.append("]");

            sendJson(exchange, "{"
                    + "\"available\":true,"
                    + "\"id\":" + jsonString(raftNode.getSelfId()) + ","
                    + "\"role\":" + jsonString(raftNode.getRole().name()) + ","
                    + "\"term\":" + raftNode.getCurrentTerm() + ","
                    + "\"leaderId\":" + jsonString(raftNode.getLeaderId()) + ","
                    + "\"commitIndex\":" + raftNode.getCommitIndex() + ","
                    + "\"lastApplied\":" + raftNode.getLastApplied() + ","
                    + "\"logSize\":" + raftNode.getLogSize() + ","
                    + "\"nextIndex\":" + jsonIntMap(raftNode.getNextIndexSnapshot()) + ","
                    + "\"matchIndex\":" + jsonIntMap(raftNode.getMatchIndexSnapshot()) + ","
                    + "\"recentEvents\":" + events
                    + "}");
        }
    }

    private static String jsonIntMap(Map<String, Integer> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append(jsonString(entry.getKey())).append(":").append(entry.getValue());
        }
        return sb.append("}").toString();
    }

    // ==================== GET /persistence ====================

    private class PersistenceHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            long walSizeBytes;
            try {
                walSizeBytes = wal.sizeInBytes();
            } catch (IOException e) {
                walSizeBytes = -1;
            }

            boolean snapshotExists = Files.exists(snapshotPath);
            long snapshotSizeBytes = -1;
            long snapshotLastModifiedMillis = -1;
            if (snapshotExists) {
                try {
                    snapshotSizeBytes = Files.size(snapshotPath);
                    snapshotLastModifiedMillis = Files.getLastModifiedTime(snapshotPath).toMillis();
                } catch (IOException ignored) {
                    // Leave the -1 defaults - a snapshot file that vanished
                    // or became unreadable between the exists() check and
                    // here is worth showing as "unavailable", not crashing over.
                }
            }

            sendJson(exchange, "{"
                    + "\"wal\":{"
                    + "\"entryCount\":" + wal.getEntryCount() + ","
                    + "\"lastWriteAtMillis\":" + wal.getLastWriteAtMillis() + ","
                    + "\"sizeBytes\":" + walSizeBytes
                    + "},"
                    + "\"snapshot\":{"
                    + "\"exists\":" + snapshotExists + ","
                    + "\"sizeBytes\":" + snapshotSizeBytes + ","
                    + "\"lastModifiedMillis\":" + snapshotLastModifiedMillis
                    + "}"
                    + "}");
        }
    }

    // ==================== POST /command ====================

    /**
     * The command console's backend. Deliberately does NOT call
     * CommandProcessor.process() directly - that would bypass this node's
     * REAL client-facing pipeline (cluster MOVED checks, and for a write,
     * Raft consensus + wait-for-commit), which would make the console
     * behave dishonestly differently from a real redis-cli session
     * against this same node. Instead, this acts as a tiny RESP CLIENT
     * itself: opens a fresh socket to THIS node's own Redis port, sends
     * the typed command as a proper RESP multibulk array (the same
     * bulk-string-array encoding ReplicationManager/RaftTransport already
     * use elsewhere in this project), and relays back whatever the real
     * server actually replies.
     */
    private class CommandHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendJson(exchange, "{\"error\":\"use POST\"}");
                return;
            }
            String commandLine = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            if (commandLine.isEmpty()) {
                sendJson(exchange, "{\"error\":\"empty command\"}");
                return;
            }

            try {
                String reply = relayCommand(commandLine);
                sendJson(exchange, "{\"ok\":true,\"reply\":" + jsonString(reply) + "}");
            } catch (IOException e) {
                sendJson(exchange, "{\"ok\":false,\"error\":" + jsonString(e.getMessage()) + "}");
            }
        }
    }

    private String relayCommand(String commandLine) throws IOException {
        // Whitespace-splitting here, at the DASHBOARD boundary - matches
        // this project's existing known limitation (RESPParser's own
        // inline-command mode has no quote-awareness either), so a value
        // containing spaces can't be sent as one argument this way. Real
        // multi-word values would need the console to accept args as a
        // list rather than one typed line - a reasonable future
        // enhancement, not implemented here.
        String[] words = commandLine.split("\\s+");
        List<CommandProcessor.CommandResult> items = new ArrayList<>(words.length);
        for (String word : words) {
            items.add(CommandProcessor.CommandResult.bulkString(word));
        }

        try (Socket socket = new Socket("localhost", redisPort)) {
            socket.setSoTimeout(3000); // generous - a Raft write can legitimately wait up to ~2s for commit
            RESPParser.writeReply(socket.getOutputStream(), CommandProcessor.CommandResult.array(items));
            return readOneReply(socket.getInputStream());
        }
    }

    /**
     * A small, purpose-built RESP REPLY reader (deliberately separate
     * from RESPParser.parseCommand, which parses INCOMING COMMANDS, not
     * general replies with their 5 different leading type bytes: '+'
     * simple string, '-' error, ':' integer, '$' bulk string, '*' array).
     * Formats each type roughly the way a real redis-cli would print it,
     * so the console output reads naturally.
     */
    private static String readOneReply(InputStream in) throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new IOException("connection closed before a reply arrived");
        }
        String line = readLine(in);
        return switch (type) {
            case '+' -> line;
            case '-' -> "(error) " + line;
            case ':' -> "(integer) " + line;
            case '$' -> {
                int length = Integer.parseInt(line);
                if (length == -1) yield "(nil)";
                byte[] data = in.readNBytes(length);
                in.readNBytes(2); // discard trailing \r\n
                yield "\"" + new String(data, StandardCharsets.UTF_8) + "\"";
            }
            case '*' -> {
                int count = Integer.parseInt(line);
                if (count == -1) yield "(nil)";
                if (count == 0) yield "(empty array)";
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < count; i++) {
                    if (i > 0) sb.append("\n");
                    sb.append(i + 1).append(") ").append(readOneReply(in));
                }
                yield sb.toString();
            }
            default -> throw new IOException("Unknown RESP reply type byte: " + (char) type);
        };
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1 && b != '\n') {
            if (b != '\r') buffer.write(b);
        }
        if (b == -1) {
            throw new IOException("connection closed mid-line");
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    // ==================== POST /admin/kill ====================

    /**
     * A REAL failure-injection switch, not a visualization-only fake:
     * responds to the caller first (so the HTTP response actually makes
     * it back before this process disappears), then exits the JVM after
     * a short delay. The other 2 members of this node's Raft group will
     * genuinely stop hearing from it, time out, and hold a real election
     * - fully observable in the existing dashboard, with nothing about it
     * simulated. There is no authentication on this endpoint - acceptable
     * ONLY because this entire dashboard is scoped to localhost for a
     * portfolio demo; it would be a real security hole on any network-
     * reachable deployment.
     */
    private class AdminKillHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendJson(exchange, "{\"error\":\"use POST\"}");
                return;
            }
            logger.warn("Received /admin/kill request - this node ({}) is about to exit deliberately.",
                    raftNode != null ? raftNode.getSelfId() : "unknown");
            sendJson(exchange, "{\"ok\":true,\"message\":\"this node will exit shortly\"}");

            Thread killThread = new Thread(() -> {
                try {
                    Thread.sleep(200); // give the HTTP response time to actually flush to the client
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                System.exit(0);
            });
            killThread.setDaemon(true);
            killThread.start();
        }
    }

    // ==================== GET / (static dashboard.html) ====================

    private class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] body = Files.readAllBytes(dashboardHtmlPath);
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }
}
