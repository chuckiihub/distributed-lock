package com.gandalf.distributed.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ValkeysServiceTest {

    @Test
    void acquiresLockAndRejectsSecondAcquisition() {
        InMemoryValkeyCommands commands = new InMemoryValkeyCommands();
        try (ValkeysService service = new ValkeysService(commands)) {
            assertTrue(service.lock("locks:alpha", Duration.ofSeconds(10)));
            assertFalse(service.lock("locks:alpha", Duration.ofSeconds(10)));
            assertTrue(service.getCurrentThreadLockToken("locks:alpha").isPresent());
        }
    }

    @Test
    void unlockOnlyDeletesWhenTokenMatches() {
        InMemoryValkeyCommands commands = new InMemoryValkeyCommands();
        try (ValkeysService service = new ValkeysService(commands)) {
            assertTrue(service.lock("locks:beta", Duration.ofSeconds(10)));
            String token = service.getCurrentThreadLockToken("locks:beta").orElseThrow();

            assertFalse(service.unlock("locks:beta", "wrong-token"));
            assertEquals(token, commands.getValue("locks:beta"));
            assertTrue(service.unlock("locks:beta", token));
            assertNull(commands.getValue("locks:beta"));
        }
    }

    @Test
    void crossThreadUnlockRequiresExplicitToken() throws InterruptedException {
        InMemoryValkeyCommands commands = new InMemoryValkeyCommands();
        try (ValkeysService service = new ValkeysService(commands)) {
            assertTrue(service.lock("locks:gamma", Duration.ofSeconds(10)));
            String token = service.getCurrentThreadLockToken("locks:gamma").orElseThrow();

            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean implicitUnlockResult = new AtomicBoolean(true);
            AtomicBoolean explicitUnlockResult = new AtomicBoolean(false);
            AtomicReference<Optional<String>> otherThreadToken = new AtomicReference<>();

            Thread thread = new Thread(() -> {
                implicitUnlockResult.set(service.unlock("locks:gamma"));
                otherThreadToken.set(service.getCurrentThreadLockToken("locks:gamma"));
                explicitUnlockResult.set(service.unlock("locks:gamma", token));
                latch.countDown();
            });
            thread.start();
            latch.await();

            assertFalse(implicitUnlockResult.get());
            assertNotNull(otherThreadToken.get());
            assertTrue(otherThreadToken.get().isEmpty());
            assertTrue(explicitUnlockResult.get());
            assertNull(commands.getValue("locks:gamma"));
        }
    }

    @Test
    void extendLockRefreshesTtlForOwnerThread() {
        InMemoryValkeyCommands commands = new InMemoryValkeyCommands();
        try (ValkeysService service = new ValkeysService(commands)) {
            assertTrue(service.lock("locks:delta", Duration.ofSeconds(5)));
            commands.advanceTime(Duration.ofSeconds(2));

            assertTrue(service.extendLock("locks:delta", Duration.ofSeconds(10)));
            Duration ttl = service.ttl("locks:delta");
            assertNotNull(ttl);
            assertEquals(Duration.ofSeconds(10), ttl);
        }
    }

    @Test
    void getCounterInitializesMissingKeyWithTtl() {
        InMemoryValkeyCommands commands = new InMemoryValkeyCommands();
        try (ValkeysService service = new ValkeysService(commands)) {
            assertEquals(10L, service.getCounter("counter:init", 10L, Duration.ofSeconds(30)));
            assertEquals(10L, service.getCounterIfPresent("counter:init"));
            assertEquals(Duration.ofSeconds(30), service.ttl("counter:init"));
        }
    }

    @Test
    void incrementCounterInitializesThenIncrementsWithoutRefreshingTtl() {
        InMemoryValkeyCommands commands = new InMemoryValkeyCommands();
        try (ValkeysService service = new ValkeysService(commands)) {
            assertEquals(8L, service.incrementCounter("counter:inc", 3L, 5L, Duration.ofSeconds(20)));

            commands.advanceTime(Duration.ofSeconds(7));
            Duration ttlAfterWait = service.ttl("counter:inc");
            assertNotNull(ttlAfterWait);
            assertEquals(Duration.ofSeconds(13), ttlAfterWait);

            assertEquals(10L, service.incrementCounter("counter:inc", 2L, 999L, Duration.ofSeconds(60)));

            Duration ttlAfterIncrement = service.ttl("counter:inc");
            assertNotNull(ttlAfterIncrement);
            assertEquals(Duration.ofSeconds(13), ttlAfterIncrement);
        }
    }

    @Test
    void ttlReturnsNullAfterKeyExpires() {
        InMemoryValkeyCommands commands = new InMemoryValkeyCommands();
        try (ValkeysService service = new ValkeysService(commands)) {
            service.setValue("values:temp", "hello", Duration.ofSeconds(3));
            assertEquals("hello", service.getValue("values:temp"));

            commands.advanceTime(Duration.ofSeconds(4));
            assertNull(service.getValue("values:temp"));
            assertNull(service.ttl("values:temp"));
        }
    }
}
