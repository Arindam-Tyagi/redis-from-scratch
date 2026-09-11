package com.redis.network;

import com.redis.core.CommandProcessor.CommandResult;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ByteArrayInputStream / ByteArrayOutputStream — first time these appear.
 * They're InputStream/OutputStream implementations backed by a plain
 * in-memory byte array instead of a real file or network socket. Since
 * RESPParser was deliberately written against the generic InputStream/
 * OutputStream interfaces (not against Socket directly), we can hand it
 * these in-memory streams here in tests and get fast, deterministic,
 * network-free verification of the exact same parsing/encoding logic that
 * will run for real once a client connects — this is precisely the payoff
 * of that earlier design decision.
 */
class RESPParserTest {

    /**
     * Hand-builds the raw RESP multibulk array bytes for a command, exactly
     * as real redis-cli would send them over the wire. We build this
     * manually (rather than using some existing "write a command" method,
     * since RESPParser only ever WRITES REPLIES, never requests — a real
     * client is responsible for writing requests, our server only reads
     * them) so the test can feed realistic wire bytes into parseCommand()
     * and check what comes out the other end.
     */
    private byte[] encodeAsRealRespCommand(String... words) {
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(words.length).append("\r\n");
        for (String word : words) {
            byte[] bytes = word.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(bytes.length).append("\r\n").append(word).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private RESPParser parserFor(byte[] input) {
        return new RESPParser(new ByteArrayInputStream(input));
    }

    private RESPParser parserFor(String rawText) {
        return parserFor(rawText.getBytes(StandardCharsets.UTF_8));
    }

    // ==================== Reading (parseCommand) ====================

    @Test
    void parsesARealRespMultibulkArrayLikeRedisCliSends() throws IOException {
        byte[] wire = encodeAsRealRespCommand("SET", "foo", "bar");
        List<String> command = parserFor(wire).parseCommand();
        assertEquals(List.of("SET", "foo", "bar"), command);
    }

    @Test
    void bulkStringLengthPrefixMeansSpacesInsideAValueAreNotSplit() throws IOException {
        // This is the whole point of length-prefixed binary-safe reading:
        // a single argument's VALUE containing spaces must come through as
        // ONE argument, not accidentally split into multiple.
        byte[] wire = encodeAsRealRespCommand("SET", "greeting", "hello world, this has spaces");
        List<String> command = parserFor(wire).parseCommand();
        assertEquals(List.of("SET", "greeting", "hello world, this has spaces"), command);
    }

    @Test
    void parsesASimpleInlineCommand() throws IOException {
        List<String> command = parserFor("PING\r\n").parseCommand();
        assertEquals(List.of("PING"), command);
    }

    @Test
    void parsesAnInlineCommandWithMultipleWords() throws IOException {
        List<String> command = parserFor("SET foo bar\r\n").parseCommand();
        assertEquals(List.of("SET", "foo", "bar"), command);
    }

    @Test
    void returnsNullOnCleanEndOfStreamMeaningClientDisconnected() throws IOException {
        List<String> command = parserFor(new byte[0]).parseCommand();
        assertNull(command);
    }

    @Test
    void malformedArrayElementThrowsProtocolException() {
        // A well-formed array header (*1) but the element inside it starts
        // with ':' (integer marker) instead of the required '$' (bulk
        // string marker) for a command argument — this must be rejected
        // cleanly, not silently misread.
        String malformed = "*1\r\n:5\r\n";
        assertThrows(RESPParser.ProtocolException.class, () -> parserFor(malformed).parseCommand());
    }

    // ==================== Writing (writeReply) ====================

    @Test
    void writesASimpleStringReply() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RESPParser.writeReply(out, CommandResult.simpleString("OK"));
        assertEquals("+OK\r\n", out.toString(StandardCharsets.UTF_8));
    }

    @Test
    void writesAnErrorReply() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RESPParser.writeReply(out, CommandResult.error("ERR something went wrong"));
        assertEquals("-ERR something went wrong\r\n", out.toString(StandardCharsets.UTF_8));
    }

    @Test
    void writesAnIntegerReply() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RESPParser.writeReply(out, CommandResult.integer(42));
        assertEquals(":42\r\n", out.toString(StandardCharsets.UTF_8));
    }

    @Test
    void writesABulkStringReply() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RESPParser.writeReply(out, CommandResult.bulkString("foo"));
        assertEquals("$3\r\nfoo\r\n", out.toString(StandardCharsets.UTF_8));
    }

    @Test
    void writesANilBulkStringReplyForAMissingKey() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RESPParser.writeReply(out, CommandResult.nilBulkString());
        assertEquals("$-1\r\n", out.toString(StandardCharsets.UTF_8));
    }

    @Test
    void writesAnArrayOfBulkStringsReply() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RESPParser.writeReply(out, CommandResult.array(List.of(
                CommandResult.bulkString("a"),
                CommandResult.bulkString("b")
        )));
        assertEquals("*2\r\n$1\r\na\r\n$1\r\nb\r\n", out.toString(StandardCharsets.UTF_8));
    }

    @Test
    void writesANilArrayReply() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RESPParser.writeReply(out, CommandResult.array(null));
        assertEquals("*-1\r\n", out.toString(StandardCharsets.UTF_8));
    }
}
