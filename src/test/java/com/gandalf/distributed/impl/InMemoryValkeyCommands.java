package com.gandalf.distributed.impl;

import com.gandalf.distributed.ValkeyCommands;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

final class InMemoryValkeyCommands implements ValkeyCommands {

    private final Map<String, Entry> entries = new HashMap<>();
    private long nowMillis;

    @Override
    public synchronized boolean setIfAbsent(String key, String value, Duration ttl) {
        purgeIfExpired(key);
        if (entries.containsKey(key)) {
            return false;
        }
        entries.put(key, new Entry(value, expiresAt(ttl)));
        return true;
    }

    @Override
    public synchronized boolean compareAndDelete(String key, String expectedValue) {
        purgeIfExpired(key);
        Entry entry = entries.get(key);
        if (entry == null || !entry.value.equals(expectedValue)) {
            return false;
        }
        entries.remove(key);
        return true;
    }

    @Override
    public synchronized boolean compareAndExpire(String key, String expectedValue, Duration ttl) {
        purgeIfExpired(key);
        Entry entry = entries.get(key);
        if (entry == null || !entry.value.equals(expectedValue)) {
            return false;
        }
        entry.expiresAtMillis = expiresAt(ttl);
        return true;
    }

    @Override
    public synchronized void setValue(String key, String value, Duration ttl) {
        entries.put(key, new Entry(value, expiresAt(ttl)));
    }

    @Override
    public synchronized String getValue(String key) {
        purgeIfExpired(key);
        Entry entry = entries.get(key);
        return entry == null ? null : entry.value;
    }

    @Override
    public synchronized boolean expire(String key, Duration ttl) {
        purgeIfExpired(key);
        Entry entry = entries.get(key);
        if (entry == null) {
            return false;
        }
        entry.expiresAtMillis = expiresAt(ttl);
        return true;
    }

    @Override
    public synchronized boolean delete(String key) {
        purgeIfExpired(key);
        return entries.remove(key) != null;
    }

    @Override
    public synchronized Duration ttl(String key) {
        purgeIfExpired(key);
        Entry entry = entries.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.expiresAtMillis == null) {
            return Duration.ZERO;
        }
        return Duration.ofMillis(Math.max(0L, entry.expiresAtMillis - nowMillis));
    }

    @Override
    public synchronized long incrementCounter(String key, long delta, long defaultValue, Duration ttl) {
        purgeIfExpired(key);
        Entry entry = entries.get(key);
        if (entry == null) {
            long initialized = defaultValue + delta;
            entries.put(key, new Entry(Long.toString(initialized), expiresAt(ttl)));
            return initialized;
        }
        long current = parseCounter(key, entry.value);
        long updated = current + delta;
        entry.value = Long.toString(updated);
        return updated;
    }

    @Override
    public synchronized long getOrInitializeCounter(String key, long defaultValue, Duration ttl) {
        purgeIfExpired(key);
        Entry entry = entries.get(key);
        if (entry == null) {
            entries.put(key, new Entry(Long.toString(defaultValue), expiresAt(ttl)));
            return defaultValue;
        }
        return parseCounter(key, entry.value);
    }

    void advanceTime(Duration duration) {
        nowMillis += duration.toMillis();
    }

    @Override
    public void close() {
    }

    private Long expiresAt(Duration ttl) {
        return nowMillis + ttl.toMillis();
    }

    private void purgeIfExpired(String key) {
        Entry entry = entries.get(key);
        if (entry != null && entry.expiresAtMillis != null && entry.expiresAtMillis <= nowMillis) {
            entries.remove(key);
        }
    }

    private long parseCounter(String key, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalStateException("Value stored at key '%s' is not a valid counter".formatted(key), exception);
        }
    }

    private static final class Entry {
        private String value;
        private Long expiresAtMillis;

        private Entry(String value, Long expiresAtMillis) {
            this.value = value;
            this.expiresAtMillis = expiresAtMillis;
        }
    }
}
