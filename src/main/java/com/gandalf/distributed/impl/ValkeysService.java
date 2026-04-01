package com.gandalf.distributed.impl;

import com.gandalf.distributed.CoordinationService;
import com.gandalf.distributed.ValkeyCommands;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Lettuce-backed coordination service for GCP Memorystore for Valkey and other Redis-compatible
 * services.
 *
 * <p>Lock ownership is tracked per thread. When a lock must be released from another thread,
 * capture the token with {@link #getCurrentThreadLockToken(String)} after acquisition and call
 * {@link #unlock(String, String)} explicitly.</p>
 */
public final class ValkeysService implements CoordinationService {

    private final ValkeyCommands commands;
    private final ThreadLocal<Map<String, String>> threadLockTokens = ThreadLocal.withInitial(HashMap::new);

    /**
     * Creates a new service using Lettuce synchronous commands.
     *
     * @param host Valkey host
     * @param port Valkey port
     * @param tlsEnabled whether TLS should be enabled
     * @param password password, nullable when authentication is not required
     */
    public ValkeysService(String host, int port, boolean tlsEnabled, String password) {
        this(new LettuceValkeyCommands(host, port, tlsEnabled, password));
    }

    ValkeysService(ValkeyCommands commands) {
        this.commands = Objects.requireNonNull(commands, "commands must not be null");
    }

    @Override
    public boolean lock(String key) {
        return lock(key, DEFAULT_LOCK_TTL);
    }

    @Override
    public boolean lock(String key, Duration ttl) {
        String normalizedKey = requireKey(key);
        Duration normalizedTtl = requirePositiveTtl(ttl, "ttl");
        String token = UUID.randomUUID().toString();
        boolean locked = commands.setIfAbsent(normalizedKey, token, normalizedTtl);
        if (locked) {
            threadLockTokens.get().put(normalizedKey, token);
        }
        return locked;
    }

    @Override
    public boolean unlock(String key) {
        String normalizedKey = requireKey(key);
        String token = threadLockTokens.get().get(normalizedKey);
        if (token == null) {
            return false;
        }
        boolean unlocked = commands.compareAndDelete(normalizedKey, token);
        threadLockTokens.get().remove(normalizedKey);
        cleanupThreadLocalIfEmpty();
        return unlocked;
    }

    @Override
    public boolean unlock(String key, String token) {
        String normalizedKey = requireKey(key);
        String normalizedToken = requireToken(token);
        boolean unlocked = commands.compareAndDelete(normalizedKey, normalizedToken);

        String ownedToken = threadLockTokens.get().get(normalizedKey);
        if (normalizedToken.equals(ownedToken)) {
            threadLockTokens.get().remove(normalizedKey);
            cleanupThreadLocalIfEmpty();
        }
        return unlocked;
    }

    @Override
    public boolean extendLock(String key, Duration ttl) {
        String normalizedKey = requireKey(key);
        Duration normalizedTtl = requirePositiveTtl(ttl, "ttl");
        String token = threadLockTokens.get().get(normalizedKey);
        if (token == null) {
            return false;
        }
        boolean extended = commands.compareAndExpire(normalizedKey, token, normalizedTtl);
        if (!extended) {
            threadLockTokens.get().remove(normalizedKey);
            cleanupThreadLocalIfEmpty();
        }
        return extended;
    }

    @Override
    public Optional<String> getCurrentThreadLockToken(String key) {
        return Optional.ofNullable(threadLockTokens.get().get(requireKey(key)));
    }

    @Override
    public void setValue(String key, String value, Duration ttl) {
        commands.setValue(requireKey(key), requireValue(value), requirePositiveTtl(ttl, "ttl"));
    }

    @Override
    public String getValue(String key) {
        return commands.getValue(requireKey(key));
    }

    @Override
    public boolean expire(String key, Duration ttl) {
        return commands.expire(requireKey(key), requirePositiveTtl(ttl, "ttl"));
    }

    @Override
    public boolean delete(String key) {
        String normalizedKey = requireKey(key);
        boolean deleted = commands.delete(normalizedKey);
        threadLockTokens.get().remove(normalizedKey);
        cleanupThreadLocalIfEmpty();
        return deleted;
    }

    @Override
    public Duration ttl(String key) {
        return commands.ttl(requireKey(key));
    }

    @Override
    public long incrementCounter(String key, long defaultValue, Duration ttl) {
        return incrementCounter(key, 1L, defaultValue, ttl);
    }

    @Override
    public long incrementCounter(String key, long delta, long defaultValue, Duration ttl) {
        return commands.incrementCounter(
                requireKey(key),
                delta,
                defaultValue,
                requirePositiveTtl(ttl, "ttl"));
    }

    @Override
    public long getCounter(String key, long defaultValue, Duration ttl) {
        return commands.getOrInitializeCounter(
                requireKey(key),
                defaultValue,
                requirePositiveTtl(ttl, "ttl"));
    }

    @Override
    public Long getCounterIfPresent(String key) {
        String value = commands.getValue(requireKey(key));
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalStateException("Value stored at key '%s' is not a valid counter".formatted(key), exception);
        }
    }

    @Override
    public void close() {
        threadLockTokens.remove();
        commands.close();
    }

    private void cleanupThreadLocalIfEmpty() {
        if (threadLockTokens.get().isEmpty()) {
            threadLockTokens.remove();
        }
    }

    private static String requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
        return key;
    }

    private static String requireValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        return value;
    }

    private static String requireToken(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("token must not be null or blank");
        }
        return token;
    }

    private static Duration requirePositiveTtl(Duration ttl, String argumentName) {
        Objects.requireNonNull(ttl, argumentName + " must not be null");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException(argumentName + " must be positive");
        }
        return ttl;
    }
}
