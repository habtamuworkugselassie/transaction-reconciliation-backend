package com.reconciliation.infrastructure.metrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessMemoryReaderTest {

    @Test
    void parsesVmRssInKilobytes() {
        List<String> status = List.of("Name:\tjava", "VmPeak:\t 9000000 kB", "VmRSS:\t  524288 kB", "Threads:\t42");

        assertThat(ProcessMemoryReader.parseVmRss(status)).hasValue(524288L * 1024);
    }

    @Test
    void missingOrMalformedVmRssIsEmpty() {
        assertThat(ProcessMemoryReader.parseVmRss(List.of("Name:\tjava"))).isEmpty();
        assertThat(ProcessMemoryReader.parseVmRss(List.of("VmRSS:\t lots kB"))).isEmpty();
        assertThat(ProcessMemoryReader.parseVmRss(List.of("VmRSS:"))).isEmpty();
    }

    @Test
    void readsFromTheGivenStatusFile(@TempDir Path dir) throws Exception {
        Path status = dir.resolve("status");
        Files.writeString(status, "Name:\tjava\nVmRSS:\t2048 kB\n");

        ProcessMemoryReader reader = new ProcessMemoryReader(status);

        assertThat(reader.available()).isTrue();
        assertThat(reader.residentBytes()).hasValue(2048L * 1024);
    }

    @Test
    void unavailableWhenTheFileDoesNotExist(@TempDir Path dir) {
        ProcessMemoryReader reader = new ProcessMemoryReader(dir.resolve("missing"));

        assertThat(reader.available()).isFalse();
        assertThat(reader.residentBytes()).isEmpty();
    }
}
