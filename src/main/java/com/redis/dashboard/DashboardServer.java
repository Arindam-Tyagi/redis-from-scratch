package com.redis.dashboard;

import com.redis.cluster.ClusterConfig;
import com.redis.core.DataStore;
import com.redis.raft.RaftNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * DashboardServer — Phase 8's first file. Exposes this ONE node's current
 * status as JSON over plain HTTP, and serves the dashboard's own static
 * HTML page — so the browser-based dashboard (the next file) can just
 * `fetch()` this URL directly, no separate file server needed.
 *
 * ===== com.sun.net.httpserver.HttpServer — a brand-new networking concept =====
 * Every server this project has built so far (Server.java, Phase 3) speaks
 * raw RESP over a plain TCP socket: WE decide the entire wire format
 * ourselves, byte by byte. HTTP is a completely different, much more
 * widely-used protocol — the same one a real web browser already knows how
 * to speak. Rather than hand-rolling an HTTP parser too (a genuinely
 * bigger undertaking than RESP, and not the point of THIS phase), the JDK
 * ships a small, built-in HTTP server in `com.sun.net.httpserver` (it's
 * not part of the officially-portable Java API - the `com.sun.*` package
 * prefix is a signal of that - but it's been bundled with every mainstream
 * JDK for years, and is more than good enough for a small internal
 * dashboard like this one; a serious public-facing production server would
 * more likely use a dedicated library instead).
 *
 * The USAGE pattern is different from ServerSocket's raw accept()-loop
 * too: instead of us writing the loop ourselves, we register HANDLERS —
 * small objects whose one job is "given one incoming request, decide what
 * to send back" — against specific URL PATHS ("/status", "/"), and the
 * HttpServer internally manages accepting connections, parsing HTTP
 * requests, and calling the right handler for each one. This is a
 * "framework calls YOUR code" style (sometimes called "inversion of
 * control"), the opposite of Server.java's "OUR code calls into the
 * framework/JDK's accept() in a loop WE wrote."
 */
public class DashboardServer {

    private static final Logger logger = LoggerFactory.getLogger(DashboardServer.class);

    private final int port;
    private final RaftNode raftNode; // nullable - same "null means off" pattern used throughout this project
    private final ClusterConfig clusterConfig; // nullable
    private final DataStore dataStore;
    private final Path dashboardHtmlPath;

    private HttpServer httpServer;

    public DashboardServer(int port, RaftNode raftNode, ClusterConfig clusterConfig, DataStore dataStore,
                            Path dashboardHtmlPath) {
        this.port = port;
        this.raftNode = raftNode;
        this.clusterConfig = clusterConfig;
        this.dataStore = dataStore;
        this.dashboardHtmlPath = dashboardHtmlPath;
    }

    /**
     * Starts listening. Unlike Server.java's start() (which BLOCKS the
     * calling thread forever inside its own accept loop), HttpServer's
     * own accept loop already runs on ITS OWN internally-managed
     * thread(s) once start() is called - so THIS method returns almost
     * immediately, and Main.java can go on to start the normal Server
     * afterward without needing a separate Thread of our own, the way we
     * had to for ReplicaClient/RaftTransport.
     */
    public void start() throws IOException {
        // `0` as the second argument means "use the OS's default TCP
        // accept backlog" - not a concept worth tuning for a small
        // internal dashboard server like this one.
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/status", new StatusHandler());
        httpServer.createContext("/", new StaticFileHandler());
        // A dedicated small thread pool (not virtual threads here - each
        // request is a quick, simple JSON/file response, not a
        // long-lived connection like a Redis client's, so there's no
        // real benefit to virtual threads' "cheap to create thousands
        // of" property in this specific case).
        httpServer.setExecutor(null); // null = HttpServer's own tiny default executor
        httpServer.start();
        logger.info("Dashboard HTTP server listening on port {}", port);
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    /**
     * Handles GET /status - returns this node's current state as a small,
     * hand-rolled JSON object. We build the JSON ourselves with plain
     * String concatenation rather than pulling in a JSON library
     * (Gson/Jackson), matching this project's established philosophy from
     * RESP and Raft's own wire format: with only a handful of FIXED
     * fields (never a variable/nested structure), there's no real
     * complexity here that a library would meaningfully simplify.
     */
    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String json = buildStatusJson();
            byte[] body = json.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            // CORS header: the dashboard's HTML page is loaded from ONE
            // node's port (e.g. 8379) but needs to fetch() status from
            // all 9 nodes' DIFFERENT ports (8379, 8382, ...). Browsers
            // treat different ports as different "origins" and block
            // cross-origin fetches by default unless the SERVER being
            // fetched from explicitly opts in via this header. "*" means
            // "any origin may read this response" - perfectly fine for a
            // read-only, non-sensitive internal status endpoint like
            // this one.
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
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

        // Plain string-building, field by field. `leaderId` needs the
        // null-becomes-JSON-null special case (JSON's `null` is written
        // as the bare word null, NOT the quoted string "null") - every
        // other field here is always present, so no other field needs
        // this same care.
        return "{"
                + "\"id\":\"" + selfId + "\","
                + "\"role\":\"" + role + "\","
                + "\"term\":" + term + ","
                + "\"leaderId\":" + (leaderId == null ? "null" : "\"" + leaderId + "\"") + ","
                + "\"slotStart\":" + slotStart + ","
                + "\"slotEnd\":" + slotEnd + ","
                + "\"keyCount\":" + keyCount
                + "}";
    }

    /**
     * Handles GET / (and any other path we didn't register a more
     * specific handler for) by serving the dashboard's static HTML file
     * straight from disk. Every one of the 9 nodes serves the EXACT SAME
     * file - it doesn't matter which node's URL you open in a browser,
     * you'll see the same dashboard, since the page's own JavaScript is
     * what fetches every node's /status individually (see dashboard.html,
     * the next file).
     */
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
