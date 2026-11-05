package com.reconciliation.application.upload;

import java.util.regex.Pattern;

/** Sanitises client-supplied file names before they are persisted as metadata. */
public final class Filenames {

    private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");
    private static final int MAX_LENGTH = 255;

    private Filenames() {
    }

    /**
     * Strips directories, control characters and anything outside a conservative
     * character set. The result is used for display only; storage keys are never
     * derived from it.
     */
    public static String sanitize(String raw) {
        if (raw == null || raw.isBlank()) {
            return "upload.ndjson";
        }
        String name = raw;
        int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
        if (slash >= 0) {
            name = name.substring(slash + 1);
        }
        name = UNSAFE.matcher(name).replaceAll("_");
        name = name.replaceAll("^\\.+", "_"); // no dot-files, no ".."
        if (name.isBlank()) {
            name = "upload.ndjson";
        }
        return name.length() > MAX_LENGTH ? name.substring(0, MAX_LENGTH) : name;
    }
}
