package com.sbytestream;

import java.time.Duration;
import java.time.Instant;

public class Stopwatch {
    public void start() {
        startedAt = Instant.now();
    }

    public long stop() {
        endedAt = Instant.now();
        return Duration.between(startedAt, endedAt).toMillis();
    }

    private Instant startedAt;
    private Instant endedAt;
}
