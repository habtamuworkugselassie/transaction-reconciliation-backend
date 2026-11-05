package com.reconciliation.api;

import com.reconciliation.application.upload.UploadSource;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.tomcat.util.http.fileupload.FileItemIterator;
import org.apache.tomcat.util.http.fileupload.FileItemStream;
import org.apache.tomcat.util.http.fileupload.FileUpload;
import org.apache.tomcat.util.http.fileupload.FileUploadException;
import org.apache.tomcat.util.http.fileupload.servlet.ServletRequestContext;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads a multipart request as a stream, part by part, without buffering any part in
 * memory or on disk. Spring's multipart resolver is disabled for this reason: it
 * would spool the whole part before the controller runs, which is both a second
 * copy on disk and a wasted upload for replayed or over-limit requests.
 *
 * <p>The first file part is handed to the caller as a lazy {@link UploadSource}.
 * Non-file fields are skipped (and capped so a hostile form can't stall the parser).
 */
final class MultipartUploadStreamer {

    private static final int MAX_FORM_FIELD_BYTES = 4096;

    /** The resolved upload: its client-declared name and content type plus the lazy body. */
    record FilePart(String filename, String contentType, UploadSource body) {}

    private MultipartUploadStreamer() {
    }

    static FilePart firstFilePart(HttpServletRequest request) throws IOException {
        String contentType = request.getContentType();
        if (contentType == null || !contentType.toLowerCase().startsWith("multipart/form-data")) {
            throw new AppException(ErrorCode.IMPORT_FILE_TYPE_INVALID, "Content-Type must be multipart/form-data");
        }
        FileUpload upload = new FileUpload();
        try {
            FileItemIterator parts = upload.getItemIterator(new ServletRequestContext(request));
            while (parts.hasNext()) {
                FileItemStream part = parts.next();
                if (part.isFormField()) {
                    try (InputStream field = part.openStream()) {
                        field.skipNBytes(Math.min(MAX_FORM_FIELD_BYTES, field.available()));
                    }
                    continue;
                }
                // The iterator is positioned on this part; the stream is valid until the
                // caller finishes reading it, which is exactly the use case's lifetime.
                return new FilePart(part.getName(), part.getContentType(), part::openStream);
            }
        } catch (FileUploadException e) {
            throw new AppException(ErrorCode.VALIDATION_FAILED, "Malformed multipart request", e);
        }
        throw new AppException(ErrorCode.IMPORT_FILE_REQUIRED, "A file part is required");
    }
}
