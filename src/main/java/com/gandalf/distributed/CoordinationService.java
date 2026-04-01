package com.gandalf.distributed;

import java.time.Duration;
import java.util.Optional;

/**
 * Plain-Java coordination service API backed by Valkey or Redis-compatible stores.
 */
public interface CoordinationService extends AutoCloseable {

    /**
     * Default TTL used by {@link #lock(String)}.
     */
    Duration DEFAULT_LOCK_TTL = Duration.ofSeconds(30);

    /**
     * Attempts to acquire a lock using the default TTL.
     *
     * @param key lock key
     * @return {@code true} when the lock is acquired
     */
    boolean lock(String key);

    /**
     * Attempts to acquire a lock using the supplied TTL.
     *
     * @param key lock key
     * @param ttl lock TTL, must be positive
     * @return {@code true} when the lock is acquired
     */
    boolean lock(String key, Duration ttl);

    /**
     * Releases a lock owned by the current thread.
     *
     * <p>The service stores lock tokens per thread, so this method only succeeds when the same
     * thread that acquired the lock calls it. Use {@link #unlock(String, String)} when ownership
     * must be transferred across threads.</p>
     *
     * @param key lock key
     * @return {@code true} when the lock was deleted
     */
    boolean unlock(String key);

    /**
     * Releases a lock using an explicit token.
     *
     * @param key lock key
     * @param token owner token previously returned by {@link #getCurrentThreadLockToken(String)}
     * @return {@code true} when the stored token matched and the key was deleted
     */
    boolean unlock(String key, String token);

    /**
     * Extends a lock owned by the current thread.
     *
     * @param key lock key
     * @param ttl new TTL, must be positive
     * @return {@code true} when the lock token matched and the TTL was updated
     */
    boolean extendLock(String key, Duration ttl);

    /**
     * Returns the lock token currently owned by this thread for the supplied key.
     *
     * @param key lock key
     * @return token for the current thread, if present
     */
    Optional<String> getCurrentThreadLockToken(String key);

    /**
     * Sets a string value with a TTL.
     *
     * @param key key name
     * @param value string value
     * @param ttl TTL, must be positive
     */
    void setValue(String key, String value, Duration ttl);

    /**
     * Reads a string value.
     *
     * @param key key name
     * @return value or {@code null} when missing
     */
    String getValue(String key);

    /**
     * Updates the TTL of an existing key.
     *
     * @param key key name
     * @param ttl TTL, must be positive
     * @return {@code true} when the TTL was updated
     */
    boolean expire(String key, Duration ttl);

    /**
     * Deletes a key.
     *
     * @param key key name
     * @return {@code true} when the key existed and was deleted
     */
    boolean delete(String key);

    /**
     * Returns the TTL of a key.
     *
     * <p>Returns {@code null} when the key does not exist and {@link Duration#ZERO} when the key
     * exists but has no expiration.</p>
     *
     * @param key key name
     * @return TTL information
     */
    Duration ttl(String key);

    /**
     * Increments a counter by {@code 1}.
     *
     * @param key counter key
     * @param defaultValue initial value used when the counter is missing
     * @param ttl TTL applied only when the counter is initialized, must be positive
     * @return current counter value after the increment
     */
    long incrementCounter(String key, long defaultValue, Duration ttl);

    /**
     * Increments a counter by the supplied delta.
     *
     * @param key counter key
     * @param delta increment amount
     * @param defaultValue initial value used when the counter is missing
     * @param ttl TTL applied only when the counter is initialized, must be positive
     * @return current counter value after the increment
     */
    long incrementCounter(String key, long delta, long defaultValue, Duration ttl);

    /**
     * Gets a counter value, initializing it when missing.
     *
     * @param key counter key
     * @param defaultValue initial value used when the counter is missing
     * @param ttl TTL applied only when the counter is initialized, must be positive
     * @return existing or initialized counter value
     */
    long getCounter(String key, long defaultValue, Duration ttl);

    /**
     * Reads a counter only when it already exists.
     *
     * @param key counter key
     * @return counter value or {@code null} when missing
     */
    Long getCounterIfPresent(String key);

    @Override
    void close();
}
