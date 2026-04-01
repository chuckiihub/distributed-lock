package com.gandalf.distributed.impl;

import com.gandalf.distributed.ValkeyCommands;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class LettuceValkeyCommands implements ValkeyCommands {

    private static final String SAFE_UNLOCK_SCRIPT =
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """;

    private static final String SAFE_EXTEND_SCRIPT =
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0
            """;

    private static final String INCREMENT_COUNTER_SCRIPT =
            """
            local current = redis.call('GET', KEYS[1])
            if not current then
              local initialized = tonumber(ARGV[1]) + tonumber(ARGV[2])
              redis.call('SET', KEYS[1], initialized)
              if tonumber(ARGV[3]) > 0 then
                redis.call('PEXPIRE', KEYS[1], ARGV[3])
              end
              return initialized
            end
            return redis.call('INCRBY', KEYS[1], tonumber(ARGV[2]))
            """;

    private static final String GET_OR_INITIALIZE_COUNTER_SCRIPT =
            """
            local current = redis.call('GET', KEYS[1])
            if current then
              return tonumber(current)
            end
            redis.call('SET', KEYS[1], tonumber(ARGV[1]))
            if tonumber(ARGV[2]) > 0 then
              redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return tonumber(ARGV[1])
            """;

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;

    LettuceValkeyCommands(String host, int port, boolean tlsEnabled, String password) {
        validateHost(host);
        validatePort(port);

        RedisURI.Builder builder = RedisURI.builder().withHost(host).withPort(port).withSsl(tlsEnabled);
        if (password != null && !password.isBlank()) {
            builder.withPassword(password.toCharArray());
        }

        this.client = RedisClient.create(builder.build());
        this.connection = client.connect();
        this.commands = connection.sync();
    }

    @Override
    public boolean setIfAbsent(String key, String value, Duration ttl) {
        String result = commands.set(key, value, SetArgs.Builder.nx().px(ttl.toMillis()));
        return "OK".equals(result);
    }

    @Override
    public boolean compareAndDelete(String key, String expectedValue) {
        Long result = commands.eval(
                SAFE_UNLOCK_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{key},
                expectedValue);
        return result != null && result == 1L;
    }

    @Override
    public boolean compareAndExpire(String key, String expectedValue, Duration ttl) {
        Long result = commands.eval(
                SAFE_EXTEND_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{key},
                expectedValue,
                Long.toString(ttl.toMillis()));
        return result != null && result == 1L;
    }

    @Override
    public void setValue(String key, String value, Duration ttl) {
        commands.set(key, value, SetArgs.Builder.px(ttl.toMillis()));
    }

    @Override
    public String getValue(String key) {
        return commands.get(key);
    }

    @Override
    public boolean expire(String key, Duration ttl) {
        return Boolean.TRUE.equals(commands.pexpire(key, ttl.toMillis()));
    }

    @Override
    public boolean delete(String key) {
        Long deleted = commands.del(key);
        return deleted != null && deleted > 0;
    }

    @Override
    public Duration ttl(String key) {
        Long ttlMillis = commands.pttl(key);
        if (ttlMillis == null || ttlMillis == -2L) {
            return null;
        }
        if (ttlMillis == -1L) {
            return Duration.ZERO;
        }
        return Duration.ofMillis(ttlMillis);
    }

    @Override
    public long incrementCounter(String key, long delta, long defaultValue, Duration ttl) {
        Long result = commands.eval(
                INCREMENT_COUNTER_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{key},
                Long.toString(defaultValue),
                Long.toString(delta),
                Long.toString(ttl.toMillis()));
        return Objects.requireNonNull(result, "Counter increment script returned null");
    }

    @Override
    public long getOrInitializeCounter(String key, long defaultValue, Duration ttl) {
        Long result = commands.eval(
                GET_OR_INITIALIZE_COUNTER_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{key},
                Long.toString(defaultValue),
                Long.toString(ttl.toMillis()));
        return Objects.requireNonNull(result, "Counter initialization script returned null");
    }

    @Override
    public void close() {
        List<RuntimeException> failures = new java.util.ArrayList<>(2);
        try {
            connection.close();
        } catch (RuntimeException exception) {
            failures.add(exception);
        }
        try {
            client.shutdown();
        } catch (RuntimeException exception) {
            failures.add(exception);
        }
        if (!failures.isEmpty()) {
            RuntimeException exception = new RuntimeException("Failed to close Valkey client cleanly");
            failures.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    private static void validateHost(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
    }
}
