package com.nadberezny.mockprom;

/** Parses Kubernetes-style quantities ({@code 1Gi}, {@code 512Mi}, {@code 1G}, {@code 1048576}). */
final class Sizes {

    private Sizes() {}

    static long parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("empty size");
        }
        String s = raw.trim();
        long multiplier = 1L;
        String number = s;

        // Longest suffixes first so "Ki" is not matched as "K".
        String[][] suffixes = {
                {"Ki", "1024"}, {"Mi", "1048576"}, {"Gi", "1073741824"}, {"Ti", "1099511627776"},
                {"K", "1000"}, {"M", "1000000"}, {"G", "1000000000"}, {"T", "1000000000000"},
        };
        for (String[] suffix : suffixes) {
            if (s.endsWith(suffix[0])) {
                multiplier = Long.parseLong(suffix[1]);
                number = s.substring(0, s.length() - suffix[0].length());
                break;
            }
        }

        double value = Double.parseDouble(number.trim());
        return (long) (value * multiplier);
    }

    /** Renders bytes back as the closest binary quantity, for human-readable JSON output. */
    static String format(long bytes) {
        String[] units = {"Ti", "Gi", "Mi", "Ki"};
        long[] scales = {1099511627776L, 1073741824L, 1048576L, 1024L};
        for (int i = 0; i < units.length; i++) {
            if (bytes >= scales[i] && bytes % scales[i] == 0) {
                return (bytes / scales[i]) + units[i];
            }
        }
        return Long.toString(bytes);
    }
}
