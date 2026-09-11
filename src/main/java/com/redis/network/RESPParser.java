package com.redis.network;

import com.redis.core.CommandProcessor.CommandResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * RESPParser — Phase 3, file 1. Handles BOTH directions of the RESP wire
 * protocol: reading a command a client sent us (parseCommand), and writing
 * a reply back to that client (writeReply).
 *
 * WHY A DEDICATED PROTOCOL CLASS, SEPARATE FROM SOCKETS:
 * This class knows nothing about sockets, connections, or threads — it only
 * knows how to read RESP-formatted bytes from an InputStream and write
 * RESP-formatted bytes to an OutputStream. InputStream and OutputStream are
 * Java's most GENERIC abstractions for "a source of bytes" and "a
 * destination for bytes" — a network socket exposes exactly these two
 * interfaces (via socket.getInputStream()/getOutputStream()), but so does a
 * plain file, an in-memory byte array, etc. By writing RESPParser purely
 * against InputStream/OutputStream instead of directly against a Socket,
 * we can (and will, in the test file) test all of this parsing/encoding
 * logic using simple in-memory byte streams — no real network connection,
 * no real client, no flaky test timing — while still being 100% the same
 * code that runs against a real socket once ClientHandler (the next file)
 * wires this together with an actual client connection.
 *
 * WHY WE CANNOT JUST USE BufferedReader/Scanner (a common instinct):
 * Those classes are built for TEXT — they assume you want to read whole
 * lines and decode bytes into characters as they go, using some charset.
 * But RESP bulk strings are declared by an exact BYTE LENGTH
 * ("$6\r\nfoobar\r\n" means "the next EXACTLY 6 bytes are the value"), and
 * that value could, in principle, contain ANY bytes at all — including
 * bytes that don't form valid text, or that happen to look like \r or \n
 * themselves. A line-based reader would get confused the instant a value
 * contains something that looks like a line ending inside it. So instead,
 * we work with the raw InputStream directly: read exactly as many bytes as
 * a length prefix says, no more, no less, and treat "how many bytes" as
 * the only thing that matters — never scanning for a delimiter inside
 * bulk string data.
 */
public class RESPParser {

    private final InputStream in;

    public RESPParser(InputStream in) {
        this.in = in;
    }

    /**
     * Reads and parses ONE full command from the stream.
     *
     * Returns null specifically to mean "the client closed the connection
     * cleanly, with nothing more to read" (InputStream.read() returning -1
     * is Java's standard way of signaling end-of-stream) — ClientHandler
     * (next file) will use a null return to know it's time to stop
     * serving this particular client and clean up.
     *
     * SUPPORTS TWO INPUT STYLES:
     * 1. The REAL RESP multibulk array format ("*3\r\n$3\r\nSET\r\n...")
     *    — this is what actual redis-cli, and any real Redis client
     *    library, always sends.
     * 2. "Inline commands" — a plain text line like "PING\r\n" with no
     *    RESP framing at all, space-separated. Real Redis supports this
     *    too, historically for very old/simple clients (like typing
     *    commands directly over a raw `telnet`/`nc` connection). We
     *    support it here mainly because it makes manually testing our
     *    server trivially easy (`echo -e "PING\r\n" | nc localhost 6379`
     *    works without needing to hand-craft RESP bytes), at very small
     *    extra code cost.
     * We tell these two apart by peeking at the very first byte: RESP
     * arrays always start with the literal character '*'; anything else
     * means "treat this whole line as an inline command instead."
     */
    public List<String> parseCommand() throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            return null; // clean disconnect
        }
        if (firstByte == '*') {
            return parseMultiBulkArray();
        }
        return parseInlineCommand(firstByte);
    }

    private List<String> parseMultiBulkArray() throws IOException {
        int argCount = parseNonNegativeInt(readLine());
        List<String> args = new ArrayList<>(argCount);

        for (int i = 0; i < argCount; i++) {
            int typeByte = in.read();
            if (typeByte != '$') {
                throw new ProtocolException(
                        "Expected bulk string ('$') as array element " + i + ", got '" + (char) typeByte + "'");
            }
            int length = parseNonNegativeInt(readLine());

            // readNBytes(n) — a convenience method added to InputStream in
            // Java 11 specifically for this exact situation. Without it,
            // you'd have to write your own loop, because a single call to
            // the more primitive in.read(byte[], offset, length) is only
            // ALLOWED to return fewer bytes than you asked for even when
            // more are coming (e.g. if the network delivers data in small
            // chunks) — it is NOT a bug for a single read() call to return
            // a "short read"; correct code has to keep calling it in a
            // loop until enough bytes have accumulated. readNBytes(n) does
            // exactly that looping for us internally, and only returns
            // fewer than n bytes if the stream genuinely ended before n
            // bytes were available.
            byte[] data = in.readNBytes(length);
            if (data.length != length) {
                throw new IOException("Unexpected end of stream: expected " + length
                        + " bytes for bulk string, got only " + data.length);
            }

            // Every bulk string's raw byte payload is followed by a
            // trailing "\r\n" in the protocol — we read and discard that
            // here (readLine() returns it as an empty string, which we
            // simply don't use, since there's nothing meaningful in it).
            readLine();

            args.add(new String(data, StandardCharsets.UTF_8));
        }

        return args;
    }

    private List<String> parseInlineCommand(int firstByte) throws IOException {
        // We already consumed `firstByte` from the stream to decide this
        // WASN'T a RESP array, so we manually prepend it back onto the rest
        // of the line before splitting on whitespace.
        String restOfLine = readLine();
        String fullLine = ((char) firstByte) + restOfLine;
        String trimmed = fullLine.trim();
        if (trimmed.isEmpty()) {
            return List.of();
        }
        // split("\\s+") — a regular expression meaning "one or more
        // whitespace characters" — splits "SET foo   bar" into
        // ["SET", "foo", "bar"] regardless of how many spaces separate
        // each word.
        return Arrays.asList(trimmed.split("\\s+"));
    }

    /**
     * Reads bytes one at a time until it finds the RESP line terminator
     * "\r\n", and returns everything before it as a String (not including
     * the \r\n itself). Used both for reading RESP's length-prefix headers
     * (like "*3" or "$6") and, doubled up with parseInlineCommand, for
     * reading a plain inline command line.
     *
     * Reading one byte at a time via in.read() is not the most performant
     * possible approach (a production system would wrap this in a
     * BufferedInputStream to reduce the number of actual system calls),
     * but it is the simplest correct approach to reason about, and RESP
     * headers are always short (a handful of bytes), so the performance
     * difference here is negligible for this project's purposes. (We may
     * revisit buffering as a Phase 10 optimization.)
     */
    private String readLine() throws IOException {
        StringBuilder line = new StringBuilder();
        int current;
        while ((current = in.read()) != -1) {
            if (current == '\r') {
                int next = in.read();
                if (next != '\n') {
                    throw new ProtocolException("Expected \\n after \\r, got '" + (char) next + "'");
                }
                break;
            }
            line.append((char) current);
        }
        return line.toString();
    }

    private int parseNonNegativeInt(String text) throws IOException {
        try {
            int value = Integer.parseInt(text);
            if (value < 0) {
                // A negative length (RESP uses -1 for "nil") is not
                // something a well-behaved client should ever send AS PART
                // OF A COMMAND (nil arrays/bulk strings are a SERVER reply
                // concept, not a client request concept) — treat it as
                // "nothing to read" rather than crashing.
                return 0;
            }
            return value;
        } catch (NumberFormatException e) {
            throw new ProtocolException("Expected an integer, got '" + text + "'");
        }
    }

    // ========================================================================
    // WRITING REPLIES — the other direction. These are all `static` because,
    // unlike reading (which needs to remember its position in an ongoing
    // stream across multiple method calls), encoding a CommandResult into
    // bytes is a pure, self-contained operation with no state to keep
    // between calls — it doesn't need an instance of RESPParser at all.
    // ========================================================================

    /**
     * Encodes `result` as RESP bytes and writes them to `out`, flushing
     * once at the end so the client actually receives the bytes right away
     * rather than having them sit in a buffer.
     */
    public static void writeReply(OutputStream out, CommandResult result) throws IOException {
        writeValue(out, result);
        out.flush();
    }

    /**
     * The recursive part, split out from writeReply specifically so ARRAY
     * replies containing NESTED CommandResults (e.g. an array of bulk
     * strings, as LRANGE/HGETALL/SMEMBERS all produce) can call this
     * directly on each inner item WITHOUT flushing after every single one —
     * we want exactly one flush, for the whole reply, done once by
     * writeReply after this recursion fully completes.
     */
    private static void writeValue(OutputStream out, CommandResult result) throws IOException {
        switch (result.getType()) {
            case SIMPLE_STRING -> writeLine(out, '+', result.getStringPayload());
            case ERROR -> writeLine(out, '-', result.getStringPayload());
            case INTEGER -> writeLine(out, ':', String.valueOf(result.getIntegerPayload()));
            case BULK_STRING -> writeBulkString(out, result.getStringPayload());
            case ARRAY -> writeArray(out, result.getArrayPayload());
        }
    }

    private static void writeLine(OutputStream out, char prefix, String content) throws IOException {
        out.write(prefix);
        out.write(content.getBytes(StandardCharsets.UTF_8));
        out.write('\r');
        out.write('\n');
    }

    private static void writeBulkString(OutputStream out, String value) throws IOException {
        if (value == null) {
            // RESP's representation of "nil" for a bulk string: a length of
            // -1, with NO actual payload or trailing \r\n after it — this
            // is genuinely a different, shorter shape than a real bulk
            // string, not "a bulk string containing nothing."
            out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.write('\r');
        out.write('\n');
    }

    private static void writeArray(OutputStream out, List<CommandResult> items) throws IOException {
        if (items == null) {
            out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8)); // RESP nil array
            return;
        }
        out.write(("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (CommandResult item : items) {
            // Recursion: each element of the array is itself a full
            // CommandResult that could, in principle, be ANY of the 5
            // types (our own commands only ever nest BULK_STRING inside
            // ARRAY today, but writing this generically costs nothing and
            // matches how RESP itself is actually defined — arrays can
            // contain any other RESP type, including other arrays).
            writeValue(out, item);
        }
    }

    /**
     * Thrown when the bytes coming from a client don't form valid RESP —
     * e.g. an array element that isn't introduced by '$', or a length
     * header that isn't a valid integer. Extends IOException (rather than
     * being a plain RuntimeException) because a protocol violation is, at
     * its core, a problem with the DATA coming off the wire — conceptually
     * the same category of problem as the stream ending unexpectedly — so
     * callers that already handle IOException from network operations
     * naturally handle this too.
     */
    public static class ProtocolException extends IOException {
        public ProtocolException(String message) {
            super(message);
        }
    }
}
