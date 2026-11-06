package com.reconciliation.infrastructure.metrics;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;

/**
 * Resident set size of this process, read from {@code /proc/self/status} ({@code VmRSS}).
 *
 * <p>The JVM's own memory metrics only cover what the JVM manages (heap, metaspace, code
 * cache, direct buffers). RSS also includes thread stacks, the JIT, GC structures and
 * native allocations, and it is what a container memory limit measures, so it is the
 * number that tells you whether the process is about to be OOM-killed.
 *
 * <p>Only Linux exposes {@code /proc}. That covers every container and the droplet. On
 * other systems {@link #available()} is false and the gauge is not registered.
 */
public final class ProcessMemoryReader {

    private static final Path STATUS = Path.of("/proc/self/status");

    private final Path status;

    public ProcessMemoryReader() {
        this(STATUS);
    }

    ProcessMemoryReader(Path status) {
        this.status = status;
    }

    public boolean available() {
        return residentBytes().isPresent();
    }

    /** Current RSS in bytes, or empty if it cannot be read on this system. */
    public OptionalLong residentBytes() {
        try {
            return parseVmRss(Files.readAllLines(status, StandardCharsets.US_ASCII));
        } catch (IOException | RuntimeException e) {
            return OptionalLong.empty();
        }
    }

    /** Parses the {@code VmRSS:   123456 kB} line; the kernel always reports it in kB. */
    static OptionalLong parseVmRss(List<String> lines) {
        for (String line : lines) {
            if (line.startsWith("VmRSS:")) {
                String[] parts = line.substring("VmRSS:".length()).trim().split("\\s+");
                if (parts.length >= 1 && !parts[0].isEmpty()) {
                    try {
                        long kb = Long.parseLong(parts[0]);
                        return OptionalLong.of(kb * 1024);
                    } catch (NumberFormatException e) {
                        return OptionalLong.empty();
                    }
                }
            }
        }
        return OptionalLong.empty();
    }
}
