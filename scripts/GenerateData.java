import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Generates an NDJSON test file with a realistic mix of valid and invalid records.
 *
 * Run with the JDK's single-file launcher (no build needed):
 *
 *   java scripts/GenerateData.java --records=500000 --out=data/transactions-500k.ndjson
 *
 * Options (all optional):
 *   --records=N          number of lines to write (default 100000)
 *   --out=PATH           output file (default data/transactions-<N>.ndjson)
 *   --invalid-rate=R     share of malformed/invalid records (default 0.02)
 *   --duplicate-rate=R   share of lines repeating an earlier transactionId (default 0.005)
 *   --seed=S             RNG seed; same seed + options = identical file (default 42)
 *   --prefix=P           transactionId prefix, so several files can be imported without
 *                        cross-file duplicates (default "txn")
 *
 * The invalid share is spread over: invalid JSON, non-object lines, missing fields,
 * wrong types, negative/zero amounts, too many decimals, unknown currencies, bad
 * timestamps, over-long descriptions, over-long lines (>16 KiB), control characters
 * and blank lines. Duplicates are half identical, half with changed content.
 */
public final class GenerateData {

    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "ETB", "KES", "JPY", "CHF", "NGN"};
    private static final String[] WORDS = {"Subscription", "payment", "Refund", "Grocery", "Transfer", "Fuel",
            "Invoice", "Taxi", "Hotel", "Coffee", "ጤና", "café", "naïve", "東京", "Zürich", "order", "#", "POS"};

    public static void main(String[] args) throws IOException {
        long records = 100_000;
        Path out = null;
        double invalidRate = 0.02;
        double duplicateRate = 0.005;
        long seed = 42;
        String prefix = "txn";
        for (String a : args) {
            String[] kv = a.split("=", 2);
            String v = kv.length > 1 ? kv[1] : "";
            switch (kv[0]) {
                case "--records" -> records = Long.parseLong(v);
                case "--out" -> out = Path.of(v);
                case "--invalid-rate" -> invalidRate = Double.parseDouble(v);
                case "--duplicate-rate" -> duplicateRate = Double.parseDouble(v);
                case "--seed" -> seed = Long.parseLong(v);
                case "--prefix" -> prefix = v;
                default -> throw new IllegalArgumentException("Unknown option " + a);
            }
        }
        if (out == null) {
            out = Path.of("data", "transactions-" + records + ".ndjson");
        }
        Files.createDirectories(out.toAbsolutePath().getParent());

        Random rnd = new Random(seed);
        List<String> recentLines = new ArrayList<>();
        long valid = 0;
        long invalid = 0;
        long duplicates = 0;
        long start = System.nanoTime();
        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            for (long i = 1; i <= records; i++) {
                double roll = rnd.nextDouble();
                String line;
                if (roll < invalidRate) {
                    line = invalidLine(rnd, prefix, i);
                    invalid++;
                } else if (roll < invalidRate + duplicateRate && !recentLines.isEmpty()) {
                    String earlier = recentLines.get(rnd.nextInt(recentLines.size()));
                    // Half the duplicates repeat the line verbatim (identical content), half
                    // change the amount (same id, different content -> conflict).
                    line = rnd.nextBoolean() ? earlier : earlier.replaceFirst("\"amount\":([0-9.]+)", "\"amount\":$1" + "1");
                    duplicates++;
                } else {
                    String id = prefix + "-" + i;
                    line = validLine(rnd, id, i);
                    if (recentLines.size() < 10_000) {
                        recentLines.add(line);
                    } else if (rnd.nextInt(50) == 0) {
                        recentLines.set(rnd.nextInt(recentLines.size()), line);
                    }
                    valid++;
                }
                w.write(line);
                if (i < records || rnd.nextInt(4) != 0) {
                    w.write('\n'); // ~25% chance the final line has no trailing newline
                }
            }
        }
        long millis = (System.nanoTime() - start) / 1_000_000;
        System.out.printf(Locale.ROOT,
                "Wrote %s%n  lines=%d valid=%d invalid=%d duplicates=%d bytes=%d in %d ms%n",
                out, records, valid, invalid, duplicates, Files.size(out), millis);
    }

    private static String validLine(Random rnd, String id, long i) {
        long accountN = 1 + rnd.nextInt(5000);
        long merchantN = 1 + rnd.nextInt(300);
        double amount = switch (rnd.nextInt(10)) {
            case 0 -> 1 + rnd.nextInt(50);
            case 1 -> 1000 + rnd.nextInt(90_000);
            case 2 -> 100_000 + rnd.nextInt(2_000_000);
            default -> 1 + rnd.nextInt(2000) + rnd.nextInt(100) / 100.0;
        };
        String currency = CURRENCIES[rnd.nextInt(CURRENCIES.length)];
        if (rnd.nextInt(40) == 0) {
            currency = currency.toLowerCase(Locale.ROOT); // normalisation case: accepted
        }
        Instant ts = Instant.parse("2026-01-01T00:00:00Z").plusSeconds(rnd.nextInt(180 * 24 * 3600));
        String timestamp = rnd.nextInt(5) == 0
                ? ts.atOffset(java.time.ZoneOffset.ofHours(3)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                : ts.toString();
        StringBuilder sb = new StringBuilder(220);
        sb.append("{\"transactionId\":\"").append(id).append("\",");
        sb.append("\"accountId\":\"acc-").append(accountN).append("\",");
        sb.append("\"merchantId\":\"merchant-").append(merchantN).append("\",");
        sb.append("\"amount\":").append(String.format(Locale.ROOT, amount % 1 == 0 ? "%.0f" : "%.2f", amount)).append(',');
        sb.append("\"currency\":\"").append(currency).append("\",");
        sb.append("\"timestamp\":\"").append(timestamp).append('"');
        int descRoll = rnd.nextInt(10);
        if (descRoll < 8) {
            sb.append(",\"description\":\"").append(description(rnd)).append('"');
        }
        if (rnd.nextInt(25) == 0) {
            sb.append(",\"channel\":\"mobile\",\"meta\":{\"batch\":").append(i % 97).append('}'); // unexpected fields
        }
        sb.append('}');
        return sb.toString();
    }

    private static String description(Random rnd) {
        int words = 1 + rnd.nextInt(6);
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < words; k++) {
            if (k > 0) {
                sb.append(rnd.nextInt(8) == 0 ? "   " : " ");
            }
            sb.append(WORDS[rnd.nextInt(WORDS.length)]);
        }
        if (rnd.nextInt(30) == 0) {
            sb.append(" ref ").append(100_000_000L + rnd.nextInt(900_000_000)); // long digit run
        }
        return sb.toString().replace("\"", "\\\"");
    }

    private static String invalidLine(Random rnd, String prefix, long i) {
        String id = prefix + "-bad-" + i;
        String base = "\"accountId\":\"acc-1\",\"merchantId\":\"merchant-1\",\"amount\":12.5,\"currency\":\"USD\",\"timestamp\":\"2026-03-01T10:00:00Z\"";
        return switch (rnd.nextInt(13)) {
            case 0 -> "{\"transactionId\":\"" + id + "\"," + base;             // truncated JSON
            case 1 -> "[1,2,3]";                                                  // not an object
            case 2 -> "{\"transactionId\":\"" + id + "\",\"accountId\":\"acc-1\",\"amount\":5,\"currency\":\"USD\",\"timestamp\":\"2026-03-01T10:00:00Z\"}"; // missing merchant
            case 3 -> "{\"transactionId\":\"" + id + "\"," + base.replace("12.5", "\"12.5\"") + "}"; // wrong type
            case 4 -> "{\"transactionId\":\"" + id + "\"," + base.replace("12.5", "-3") + "}";       // negative
            case 5 -> "{\"transactionId\":\"" + id + "\"," + base.replace("12.5", "1.23456") + "}";  // too many decimals
            case 6 -> "{\"transactionId\":\"" + id + "\"," + base.replace("\"USD\"", "\"US\"") + "}";  // bad currency
            case 7 -> "{\"transactionId\":\"" + id + "\"," + base.replace("2026-03-01T10:00:00Z", "yesterday") + "}";
            case 8 -> "{\"transactionId\":\"" + id + "\"," + base + ",\"description\":\"" + "x".repeat(520) + "\"}";
            case 9 -> "{\"transactionId\":\"" + id + "\"," + base + ",\"description\":\"" + "y".repeat(20_000) + "\"}"; // too long line
            case 10 -> "{\"transactionId\":\"" + id + "\\u0000\"," + base + "}";  // control char in id
            case 11 -> "";                                                        // blank line (skipped, not counted)
            default -> "not json at all " + i;
        };
    }
}
