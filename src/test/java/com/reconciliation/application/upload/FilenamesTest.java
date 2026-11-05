package com.reconciliation.application.upload;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FilenamesTest {

    @Test
    void keepsSafeNames() {
        assertThat(Filenames.sanitize("transactions-2026_07.ndjson")).isEqualTo("transactions-2026_07.ndjson");
    }

    @Test
    void stripsDirectoriesAndPathTraversal() {
        assertThat(Filenames.sanitize("../../etc/passwd")).isEqualTo("passwd");
        assertThat(Filenames.sanitize("C:\\Users\\me\\file.ndjson")).isEqualTo("file.ndjson");
        assertThat(Filenames.sanitize("..")).isEqualTo("_");
        assertThat(Filenames.sanitize(".hidden")).isEqualTo("_hidden");
    }

    @Test
    void replacesControlAndSpecialCharacters() {
        assertThat(Filenames.sanitize("my file\n\u0000;rm -rf.ndjson")).isEqualTo("my_file___rm_-rf.ndjson");
        assertThat(Filenames.sanitize("日本.ndjson")).isEqualTo("__.ndjson");
    }

    @Test
    void fallsBackForMissingOrUnusableNames() {
        assertThat(Filenames.sanitize(null)).isEqualTo("upload.ndjson");
        assertThat(Filenames.sanitize("   ")).isEqualTo("upload.ndjson");
        assertThat(Filenames.sanitize("/")).isEqualTo("upload.ndjson");
    }

    @Test
    void truncatesTo255Characters() {
        assertThat(Filenames.sanitize("a".repeat(300))).hasSize(255);
    }
}
