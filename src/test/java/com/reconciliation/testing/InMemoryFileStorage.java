package com.reconciliation.testing;

import com.reconciliation.application.port.FileStorage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Byte arrays in a map. Can be told to fail on open to simulate a storage outage. */
public final class InMemoryFileStorage implements FileStorage {

    private final Map<String, byte[]> files = new ConcurrentHashMap<>();
    public volatile boolean writable = true;
    public volatile IOException failOnOpen;

    @Override
    public long store(String key, InputStream content) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        content.transferTo(out);
        files.put(key, out.toByteArray());
        return out.size();
    }

    @Override
    public InputStream open(String key) throws IOException {
        if (failOnOpen != null) {
            throw failOnOpen;
        }
        byte[] bytes = files.get(key);
        if (bytes == null) {
            throw new IOException("No such key: " + key);
        }
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public void delete(String key) {
        files.remove(key);
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    public void put(String key, String text) {
        files.put(key, text.getBytes(StandardCharsets.UTF_8));
    }

    public boolean contains(String key) {
        return files.containsKey(key);
    }

    public String text(String key) {
        byte[] b = files.get(key);
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    public int size() {
        return files.size();
    }
}
