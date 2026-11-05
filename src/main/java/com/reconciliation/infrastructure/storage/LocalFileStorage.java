package com.reconciliation.infrastructure.storage;

import com.reconciliation.application.port.FileStorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Filesystem-backed storage under a fixed root.
 *
 * <p>Writes go to {@code <key>.part} and are atomically renamed on success, so a
 * reader can never observe a truncated file and a crash leaves at most an orphaned
 * {@code .part}, which {@link #delete} and {@link #sweepPartials()} remove. Keys are
 * validated against the root so a malicious key can't escape it even if a caller
 * were ever to pass client data.
 */
public final class LocalFileStorage implements FileStorage {

    private static final int BUFFER = 64 * 1024;

    private final Path root;

    public LocalFileStorage(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize();
        Files.createDirectories(this.root);
    }

    @Override
    public long store(String key, InputStream content) throws IOException {
        Path target = resolve(key);
        Path partial = target.resolveSibling(target.getFileName() + ".part");
        Files.createDirectories(target.getParent());
        long written = 0;
        try (OutputStream out = Files.newOutputStream(partial, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            byte[] buf = new byte[BUFFER];
            int n;
            while ((n = content.read(buf)) > 0) {
                out.write(buf, 0, n);
                written += n;
            }
        } catch (IOException e) {
            Files.deleteIfExists(partial);
            throw e;
        }
        try {
            Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(partial, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return written;
    }

    @Override
    public InputStream open(String key) throws IOException {
        return Files.newInputStream(resolve(key), StandardOpenOption.READ);
    }

    @Override
    public void delete(String key) throws IOException {
        Path target = resolve(key);
        Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
        Files.deleteIfExists(target);
    }

    @Override
    public boolean isWritable() {
        return Files.isDirectory(root) && Files.isWritable(root);
    }

    public Path root() {
        return root;
    }

    private Path resolve(String key) {
        if (key == null || key.isBlank() || key.contains("..") || key.startsWith("/")) {
            throw new IllegalArgumentException("Invalid storage key");
        }
        Path resolved = root.resolve(key).normalize();
        if (!resolved.startsWith(root)) {
            throw new IllegalArgumentException("Storage key escapes root");
        }
        return resolved;
    }
}
