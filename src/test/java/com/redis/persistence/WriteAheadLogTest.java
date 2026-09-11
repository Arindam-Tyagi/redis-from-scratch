package com.redis.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @TempDir — a JUnit 5 annotation that injects a fresh, temporary directory
 * (created on disk just for this one test method, and automatically
 * deleted afterward) into the annotated field or parameter. This matters
 * here specifically: without it, our WAL tests would need to write real
 * files somewhere, and if we hardcoded a path like "wal/test.wal", running
 * tests repeatedly would either need manual cleanup between runs, or worse,
 * could collide with a real WAL file the actual server is using. @TempDir
 * gives every test method its own private, disposable folder — no cleanup
 * code needed, no collision risk, and no leftover files after `mvn test`
 * finishes.
 */
class WriteAheadLogTest {

    @TempDir
    Path tempDir;

    @Test
    void appendedCommandsSurviveBeingReadBackByABrandNewInstance() throws IOException {
        Path walFile = tempDir.resolve("test.wal");

        // First "process run": open the WAL and append some commands.
        WriteAheadLog firstInstance = new WriteAheadLog(walFile);
        firstInstance.append(List.of("SET", "foo", "bar"));
        firstInstance.append(List.of("LPUSH", "mylist", "a", "b", "c"));
        firstInstance.append(List.of("DEL", "foo"));
        firstInstance.close();

        // Second "process run": brand new WriteAheadLog OBJECT pointing at
        // the SAME file — simulating what happens after a real restart,
        // where nothing from the old JVM's memory survives, only the file
        // on disk does.
        WriteAheadLog secondInstance = new WriteAheadLog(walFile);
        List<List<String>> replayed = secondInstance.readAll();
        secondInstance.close();

        assertEquals(3, replayed.size());
        assertEquals(List.of("SET", "foo", "bar"), replayed.get(0));
        assertEquals(List.of("LPUSH", "mylist", "a", "b", "c"), replayed.get(1));
        assertEquals(List.of("DEL", "foo"), replayed.get(2));
    }

    @Test
    void freshWalWithNothingAppendedYetReadsBackAsEmpty() throws IOException {
        Path walFile = tempDir.resolve("empty.wal");
        WriteAheadLog wal = new WriteAheadLog(walFile);

        List<List<String>> replayed = wal.readAll();

        assertTrue(replayed.isEmpty());
        wal.close();
    }

    @Test
    void sizeInBytesGrowsAsCommandsAreAppended() throws IOException {
        Path walFile = tempDir.resolve("size.wal");
        WriteAheadLog wal = new WriteAheadLog(walFile);

        long sizeBeforeAnyAppend = wal.sizeInBytes();
        wal.append(List.of("SET", "a", "1"));
        long sizeAfterOneAppend = wal.sizeInBytes();
        wal.append(List.of("SET", "b", "2"));
        long sizeAfterTwoAppends = wal.sizeInBytes();

        assertTrue(sizeAfterOneAppend > sizeBeforeAnyAppend);
        assertTrue(sizeAfterTwoAppends > sizeAfterOneAppend);

        wal.close();
    }

    @Test
    void valuesContainingSpacesAndNewlinesRoundTripCorrectly() throws IOException {
        // This is the whole point of length-prefixed encoding instead of
        // splitting on spaces: a value that itself CONTAINS spaces, or even
        // a newline character, must come back exactly as it went in.
        Path walFile = tempDir.resolve("special-chars.wal");
        String trickyValue = "hello world\nwith a newline and    multiple   spaces";

        WriteAheadLog writer = new WriteAheadLog(walFile);
        writer.append(List.of("SET", "tricky", trickyValue));
        writer.close();

        WriteAheadLog reader = new WriteAheadLog(walFile);
        List<List<String>> replayed = reader.readAll();
        reader.close();

        assertEquals(1, replayed.size());
        assertEquals(trickyValue, replayed.get(0).get(2));
    }
}
