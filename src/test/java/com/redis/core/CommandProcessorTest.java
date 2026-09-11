package com.redis.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * First real test file in this project — worth explaining the mechanics
 * before the actual test methods, since this is new territory.
 *
 * WHERE THIS FILE LIVES MATTERS:
 * Maven has a strict convention: production code goes under
 * src/main/java/..., test code goes under src/test/java/... — using the
 * EXACT SAME package name (com.redis.core here) as the class being tested.
 * Maven's compiler plugin compiles src/main/java separately from its
 * surefire plugin, which compiles AND RUNS src/test/java — test code is
 * never bundled into the final production JAR (remember the maven-shade-
 * plugin from pom.xml? it only ever packages src/main/java's output). This
 * is why we made junit-jupiter's scope "test" in pom.xml.
 *
 * @Test — an ANNOTATION (the @ symbol). An annotation is metadata attached
 *       to a method/class/field that some other tool reads and acts on — it
 *       adds
 *       no behavior by itself. Here, @Test tells JUnit's test runner (invoked
 *       by
 *       the surefire plugin during `mvn test`) "this method is a test case, run
 *       it and report whether it passes or fails." Without @Test, a method in
 *       this file is just a regular unused method — JUnit would never call it.
 *
 *       assertEquals(expected, actual) — the core JUnit building block. It
 *       compares two values; if they don't match, the test FAILS immediately
 *       with a message showing exactly what was expected vs what was actually
 *       received, and JUnit moves on to the next test method (one failing
 *       assertion does not stop other test methods from running).
 *
 * @BeforeEach — another annotation. JUnit creates a BRAND NEW instance of
 *             this whole test class for EVERY single @Test method (this is
 *             intentional
 *             test isolation — it stops one test's leftover state from silently
 *             affecting another test that happens to run after it). A method
 *             marked
 * @BeforeEach runs automatically right before each and every @Test method,
 *             on that fresh instance — perfect for "set up a clean DataStore
 *             and
 *             CommandProcessor so every test starts from an empty database."
 */
class CommandProcessorTest {

    private DataStore dataStore;
    private CommandProcessor commandProcessor;

    @BeforeEach
    void setUp() {
        dataStore = new DataStore();
        commandProcessor = new CommandProcessor(dataStore);
    }

    /**
     * Small helper so test methods can write process("SET", "foo", "bar")
     * instead of the more verbose process(List.of("SET", "foo", "bar"))
     * every single time. `String... words` is Java's "varargs" syntax —
     * it lets a method be called with any number of String arguments
     * (zero, one, or many), which Java automatically collects into a
     * String[] array inside the method. List.of(...) then converts that
     * array into an immutable List, matching what CommandProcessor.process
     * actually expects.
     */
    private CommandProcessor.CommandResult run(String... words) {
        return commandProcessor.process(List.of(words));
    }

    @Test
    void setThenGetReturnsTheStoredValue() {
        CommandProcessor.CommandResult setResult = run("SET", "foo", "bar");
        assertEquals(CommandProcessor.CommandResult.Type.SIMPLE_STRING, setResult.getType());
        assertEquals("OK", setResult.getStringPayload());

        CommandProcessor.CommandResult getResult = run("GET", "foo");
        assertEquals(CommandProcessor.CommandResult.Type.BULK_STRING, getResult.getType());
        assertEquals("bar", getResult.getStringPayload());
    }

    @Test
    void getOnMissingKeyReturnsNilBulkString() {
        CommandProcessor.CommandResult result = run("GET", "doesNotExist");
        assertEquals(CommandProcessor.CommandResult.Type.BULK_STRING, result.getType());
        // A nil bulk string is represented as BULK_STRING type with a null
        // payload — assertNull checks exactly that.
        assertNull(result.getStringPayload());
    }

    @Test
    void delReturnsCountOfKeysActuallyRemoved() {
        run("SET", "a", "1");
        run("SET", "b", "2");
        // "c" is never set, so DEL should only count "a" and "b".
        CommandProcessor.CommandResult result = run("DEL", "a", "b", "c");
        assertEquals(2, result.getIntegerPayload());
        assertEquals(CommandProcessor.CommandResult.Type.BULK_STRING, run("GET", "a").getType());
        assertNull(run("GET", "a").getStringPayload());
    }

    @Test
    void wrongTypeOperationReturnsAnErrorInsteadOfCrashing() {
        run("SET", "mystring", "hello");
        // LPUSH against a key holding a STRING must fail cleanly with a
        // WRONGTYPE-style error, not throw an uncaught exception up through
        // process() — this exercises the RedisValue.WrongTypeException ->
        // process()'s catch block -> CommandResult.error(...) path end to end.
        CommandProcessor.CommandResult result = run("LPUSH", "mystring", "x");
        assertEquals(CommandProcessor.CommandResult.Type.ERROR, result.getType());
        assertTrue(result.getStringPayload().startsWith("WRONGTYPE"));
    }

    @Test
    void lpushAddsToFrontInReverseArgumentOrder() {
        // LPUSH mylist a b c should leave the list as [c, b, a] — see the
        // comment in CommandProcessor.push() for why.
        run("LPUSH", "mylist", "a", "b", "c");
        CommandProcessor.CommandResult range = run("LRANGE", "mylist", "0", "-1");
        assertEquals(CommandProcessor.CommandResult.Type.ARRAY, range.getType());
        List<CommandProcessor.CommandResult> items = range.getArrayPayload();
        assertEquals(3, items.size());
        assertEquals("c", items.get(0).getStringPayload());
        assertEquals("b", items.get(1).getStringPayload());
        assertEquals("a", items.get(2).getStringPayload());
    }

    @Test
    void rpushAddsToTailInGivenOrder() {
        run("RPUSH", "mylist", "a", "b", "c");
        List<CommandProcessor.CommandResult> items = run("LRANGE", "mylist", "0", "-1").getArrayPayload();
        assertEquals(List.of("a", "b", "c"),
                items.stream().map(CommandProcessor.CommandResult::getStringPayload).toList());
    }

    @Test
    void lpopAndRpopRemoveFromCorrectEndsAndDeleteKeyWhenEmpty() {
        run("RPUSH", "mylist", "a", "b", "c");

        assertEquals("a", run("LPOP", "mylist").getStringPayload());
        assertEquals("c", run("RPOP", "mylist").getStringPayload());
        // Only "b" left now.
        assertEquals(1, run("LLEN", "mylist").getIntegerPayload());

        assertEquals("b", run("LPOP", "mylist").getStringPayload());
        // List is now empty -> the key itself should be gone entirely.
        assertEquals(0, run("LLEN", "mylist").getIntegerPayload());
        assertEquals("none", run("TYPE", "mylist").getStringPayload());
    }

    @Test
    void hsetHgetAndHgetallWorkTogether() {
        CommandProcessor.CommandResult hsetResult = run("HSET", "user:1", "name", "Ajju", "role", "developer");
        assertEquals(2, hsetResult.getIntegerPayload()); // 2 brand-new fields

        assertEquals("Ajju", run("HGET", "user:1", "name").getStringPayload());

        // Updating an EXISTING field should NOT count as a new field added.
        CommandProcessor.CommandResult updateResult = run("HSET", "user:1", "name", "Ajju Singh");
        assertEquals(0, updateResult.getIntegerPayload());
        assertEquals("Ajju Singh", run("HGET", "user:1", "name").getStringPayload());

        List<CommandProcessor.CommandResult> all = run("HGETALL", "user:1").getArrayPayload();
        assertEquals(4, all.size()); // flattened: field, value, field, value
    }

    @Test
    void saddSremAndSismemberTrackMembershipCorrectly() {
        CommandProcessor.CommandResult addResult = run("SADD", "tags", "java", "redis", "java");
        // "java" appears twice in the same SADD call -> only counted once as newly
        // added.
        assertEquals(2, addResult.getIntegerPayload());

        assertEquals(1, run("SISMEMBER", "tags", "redis").getIntegerPayload());
        assertEquals(0, run("SISMEMBER", "tags", "python").getIntegerPayload());

        run("SREM", "tags", "redis");
        assertEquals(0, run("SISMEMBER", "tags", "redis").getIntegerPayload());
    }

    @Test
    void unknownCommandReturnsAnError() {
        CommandProcessor.CommandResult result = run("FOOBAR", "x");
        assertEquals(CommandProcessor.CommandResult.Type.ERROR, result.getType());
        assertTrue(result.getStringPayload().contains("unknown command"));
    }
}
