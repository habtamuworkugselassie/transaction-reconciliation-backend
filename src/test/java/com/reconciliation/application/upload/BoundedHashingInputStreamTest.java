package com.reconciliation.application.upload;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedHashingInputStreamTest {

    @Test
    void countsAndHashesEverythingThatFlowsThrough() throws IOException {
        byte[] data = "hello world\n".getBytes(StandardCharsets.UTF_8);
        BoundedHashingInputStream in = new BoundedHashingInputStream(new ByteArrayInputStream(data), 1024);

        byte[] read = in.readAllBytes();

        assertThat(read).isEqualTo(data);
        assertThat(in.count()).isEqualTo(data.length);
        // sha256("hello world\n")
        assertThat(in.sha256Hex()).isEqualTo("a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447");
    }

    @Test
    void singleByteReadsAreAccountedToo() throws IOException {
        BoundedHashingInputStream in = new BoundedHashingInputStream(new ByteArrayInputStream(new byte[]{1, 2, 3}), 10);
        while (in.read() >= 0) {
            // drain
        }
        assertThat(in.count()).isEqualTo(3);
    }

    @Test
    void stopsAtTheLimitMidStream() {
        byte[] data = new byte[10_000];
        BoundedHashingInputStream in = new BoundedHashingInputStream(new ByteArrayInputStream(data), 4096);

        assertThatThrownBy(in::readAllBytes).isInstanceOf(BoundedHashingInputStream.LimitExceededException.class);
        assertThat(in.count()).as("the read that crossed the limit is the last one").isGreaterThan(4096);
    }

    @Test
    void exactlyTheLimitIsAllowed() throws IOException {
        byte[] data = new byte[4096];
        BoundedHashingInputStream in = new BoundedHashingInputStream(new ByteArrayInputStream(data), 4096);

        assertThat(in.readAllBytes()).hasSize(4096);
    }
}
