package com.reconciliation.infrastructure.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalFileStorageTest {

    @TempDir
    Path root;

    @Test
    void storesUnderRootAndReadsBack() throws IOException {
        LocalFileStorage storage = new LocalFileStorage(root);

        long written = storage.store("imports/a.ndjson", new ByteArrayInputStream("{}\n".getBytes(StandardCharsets.UTF_8)));

        assertThat(written).isEqualTo(3);
        assertThat(Files.readString(root.resolve("imports/a.ndjson"))).isEqualTo("{}\n");
        try (InputStream in = storage.open("imports/a.ndjson")) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("{}\n");
        }
        assertThat(storage.isWritable()).isTrue();
    }

    @Test
    void aFailedWriteLeavesNoPartialFile() throws IOException {
        LocalFileStorage storage = new LocalFileStorage(root);
        InputStream failing = new InputStream() {
            int n = 0;
            @Override public int read() throws IOException {
                if (n++ > 10) throw new IOException("client went away");
                return 'x';
            }
        };

        assertThatThrownBy(() -> storage.store("imports/b.ndjson", failing)).isInstanceOf(IOException.class);

        assertThat(Files.exists(root.resolve("imports/b.ndjson"))).isFalse();
        assertThat(Files.exists(root.resolve("imports/b.ndjson.part"))).isFalse();
    }

    @Test
    void deleteRemovesFileAndOrphanedPartialAndIgnoresMissing() throws IOException {
        LocalFileStorage storage = new LocalFileStorage(root);
        storage.store("imports/c.ndjson", new ByteArrayInputStream(new byte[]{1}));
        Files.writeString(root.resolve("imports/c.ndjson.part"), "orphan");

        storage.delete("imports/c.ndjson");
        storage.delete("imports/never-existed.ndjson");

        assertThat(Files.exists(root.resolve("imports/c.ndjson"))).isFalse();
        assertThat(Files.exists(root.resolve("imports/c.ndjson.part"))).isFalse();
    }

    @Test
    void keysCannotEscapeTheRoot() throws IOException {
        LocalFileStorage storage = new LocalFileStorage(root);

        assertThatThrownBy(() -> storage.open("../outside")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> storage.open("/etc/passwd")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> storage.open("a/../../b")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> storage.open("")).isInstanceOf(IllegalArgumentException.class);
    }
}
