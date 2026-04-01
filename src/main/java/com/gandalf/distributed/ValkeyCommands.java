package com.gandalf.distributed;

import java.time.Duration;

public interface ValkeyCommands extends AutoCloseable {

    boolean setIfAbsent(String key, String value, Duration ttl);

    boolean compareAndDelete(String key, String expectedValue);

    boolean compareAndExpire(String key, String expectedValue, Duration ttl);

    void setValue(String key, String value, Duration ttl);

    String getValue(String key);

    boolean expire(String key, Duration ttl);

    boolean delete(String key);

    Duration ttl(String key);

    long incrementCounter(String key, long delta, long defaultValue, Duration ttl);

    long getOrInitializeCounter(String key, long defaultValue, Duration ttl);

    @Override
    void close();
}
