# Valkeys Service

`distributed-lock` is a small Java 21 library for coordination primitives backed by Valkey or Redis-compatible stores. It is designed as plain Java first, uses Maven for builds, and talks to GCP Memorystore for Valkey through the Lettuce synchronous API.

GCP Memorystore for Valkey is Redis-compatible, so a standard Redis client such as Lettuce can be used for locking, counters, and expiring key/value entries.

## Features

- Distributed locks built on `SET key value NX PX ttl`
- Safe unlock and lock extension using Lua scripts
- Per-thread lock token tracking for plain Java usage
- Generic string key/value storage with TTL support
- Atomic counters that initialize on first access

## Maven

```xml
<dependency>
    <groupId>com.gandalf</groupId>
    <artifactId>distributed-lock</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Publish To Artifact Registry

Create a standard Maven repository in `gxperts-bls-dev`:

```bash
gcloud artifacts repositories create distributed-lock \
  --project=gxperts-bls-dev \
  --repository-format=maven \
  --location=europe-west6 \
  --description="Shared Java libraries"
```

Authenticate Maven for that repository:

```bash
gcloud auth application-default login
gcloud artifacts print-settings mvn \
  --project=gxperts-bls-dev \
  --repository=distributed-lock \
  --location=europe-west6
```

Add the generated `server`, `repository`, and `profile` entries to `~/.m2/settings.xml`.

This repository already includes the required Maven core extension in `.mvn/extensions.xml` for `artifactregistry://` URLs.

Deploy this library:

```bash
mvn \
  -Partifact-registry \
  deploy
```

This repository keeps the deploy target in `pom.xml` and leaves the Artifact Registry authentication helper setup to the generated Google Cloud snippet so normal local builds are not coupled to GCP access.

## Consume From Other Projects

In the consuming project, add the generated Artifact Registry Maven settings from:

```bash
gcloud artifacts print-settings mvn \
  --project=gxperts-bls-dev \
  --repository=distributed-lock \
  --location=europe-west6
```

Then use the dependency:

```xml
<dependency>
    <groupId>com.gandalf</groupId>
    <artifactId>distributed-lock</artifactId>
    <version>1.0.0</version>
</dependency>
```

The runtime transitive dependency on Lettuce will be resolved through the published POM.

## Usage

```java
import com.gandalf.distributed.impl.ValkeysService;
import java.time.Duration;

try(ValkeysService service = new ValkeysService(
    "10.0.0.5",
    6379,
    true,
    "secret-password")){

    if(service.

lock("locks:job-42",Duration.ofSeconds(30))){
    try{
    service.

setValue("jobs:42:status","running",Duration.ofMinutes(5));

long current = service.incrementCounter(
    "jobs:42:attempts",
    1L,
    0L,
    Duration.ofHours(1));

            System.out.

println("Attempts: "+current);
        }finally{
            service.

unlock("locks:job-42");
        }
            }
            }
```

## Lock Semantics

Locks use the usual Redis or Valkey pattern:

```text
SET key value NX PX ttl
```

Each successful lock acquisition generates a unique token. `unlock(key)` is safe because it uses a Lua script that deletes the key only when the stored token matches the caller's token.

`ValkeysService` stores lock tokens per thread. That makes `unlock(key)` convenient in plain Java code, but it also means:

- `unlock(key)` only works from the same thread that acquired the lock
- cross-thread unlocks must use `unlock(key, token)`
- after acquiring a lock, use `getCurrentThreadLockToken(key)` if you need to pass ownership metadata to another thread

Example:

```java
String key = "locks:job-42";
if (service.lock(key, Duration.ofSeconds(30))) {
    String token = service.getCurrentThreadLockToken(key).orElseThrow();
    // Pass `token` to another thread if that thread must release the lock:
    service.unlock(key, token);
}
```

## Counter Semantics

- `getCounter(key, defaultValue, ttl)` initializes the counter to `defaultValue` when missing
- `incrementCounter(key, delta, defaultValue, ttl)` initializes missing counters to `defaultValue + delta`
- TTL is applied only when the counter is first created
- Existing counters do not automatically refresh their TTL on reads or increments

## Caveats

- Lock expiry is time-based. If the TTL expires before work finishes, another client can acquire the same lock.
- If work may exceed the lock TTL, call `extendLock(key, ttl)` from the owning thread before expiry.
- Cross-thread unlock is explicit by design. Use the lock token when ownership must move between threads.
- `ttl(key)` returns `null` for missing keys and `Duration.ZERO` for keys that exist without an expiration.

## Running Tests

```bash
mvn test
```
