package com.nadberezny.mockprom;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

final class PromQl {

    static final String USED = "kubelet_volume_stats_used_bytes";
    static final String CAPACITY = "kubelet_volume_stats_capacity_bytes";
    static final String AVAILABLE = "kubelet_volume_stats_available_bytes";

    private PromQl() {}

    static final class BadQueryException extends RuntimeException {
        BadQueryException(String message) {
            super(message);
        }
    }

    record Sample(Map<String, String> labels, double value) {}

    private record Matcher(String label, String op, String value, Pattern pattern) {
        boolean matches(Map<String, String> labels) {
            String actual = labels.getOrDefault(label, "");
            return switch (op) {
                case "=" -> actual.equals(value);
                case "!=" -> !actual.equals(value);
                case "=~" -> pattern.matcher(actual).matches();
                case "!~" -> !pattern.matcher(actual).matches();
                default -> throw new BadQueryException("unsupported matcher operator '" + op + "'");
            };
        }
    }

    private record Selector(String metric, List<Matcher> matchers) {
        /** The exact value a query pins a label to, if it does so with {@code =}. */
        String exact(String label) {
            for (Matcher m : matchers) {
                if (m.label().equals(label) && m.op().equals("=")) {
                    return m.value();
                }
            }
            return null;
        }
    }

    static List<Sample> evaluate(String query, MetricStore store) {
        if (query == null || query.isBlank()) {
            throw new BadQueryException("empty query");
        }
        List<String> operands = splitTopLevel(query, '/');
        if (operands.size() == 1) {
            return select(parseSelector(operands.get(0)), store);
        }
        if (operands.size() == 2) {
            return divide(parseSelector(operands.get(0)), parseSelector(operands.get(1)), store);
        }
        throw new BadQueryException("only a single '/' between two selectors is supported");
    }

    private static List<Sample> divide(Selector lhs, Selector rhs, MetricStore store) {
        Map<String, Double> denominators = new LinkedHashMap<>();
        for (Sample s : select(rhs, store)) {
            denominators.put(joinKey(s.labels()), s.value());
        }

        List<Sample> result = new ArrayList<>();
        for (Sample s : select(lhs, store)) {
            Double denominator = denominators.get(joinKey(s.labels()));
            if (denominator == null) {
                // Prometheus drops unmatched series on the left-hand side.
                continue;
            }
            if (denominator == 0.0) {
                continue;
            }
            // Prometheus strips __name__ from the result of binary arithmetic.
            Map<String, String> labels = new LinkedHashMap<>(s.labels());
            labels.remove("__name__");
            result.add(new Sample(labels, s.value() / denominator));
        }
        return result;
    }

    /** Series are matched across the two operands on their identifying labels. */
    private static String joinKey(Map<String, String> labels) {
        return labels.getOrDefault("namespace", "") + "/"
                + labels.getOrDefault("persistentvolumeclaim", "");
    }

    private static List<Sample> select(Selector selector, MetricStore store) {
        if (!selector.metric().equals(USED)
                && !selector.metric().equals(CAPACITY)
                && !selector.metric().equals(AVAILABLE)) {
            throw new BadQueryException("unknown metric '" + selector.metric()
                    + "'; this mock only serves " + USED + ", " + CAPACITY + ", " + AVAILABLE);
        }

        store.maybeAutoRegister(selector.exact("namespace"), selector.exact("persistentvolumeclaim"));

        long now = store.now();
        List<Sample> samples = new ArrayList<>();
        for (VolumeSeries volume : store.all()) {
            Map<String, String> labels = new LinkedHashMap<>();
            labels.put("__name__", selector.metric());
            labels.putAll(volume.labels());

            boolean matched = true;
            for (Matcher matcher : selector.matchers()) {
                if (!matcher.matches(labels)) {
                    matched = false;
                    break;
                }
            }
            if (!matched) {
                continue;
            }

            double value = switch (selector.metric()) {
                case USED -> volume.usedBytes(now);
                case CAPACITY -> volume.capacityBytes();
                default -> volume.availableBytes(now);
            };
            samples.add(new Sample(labels, value));
        }
        return samples;
    }

    private static Selector parseSelector(String raw) {
        String s = raw.trim();
        int brace = s.indexOf('{');
        if (brace < 0) {
            return new Selector(validateMetricName(s), List.of());
        }
        if (!s.endsWith("}")) {
            throw new BadQueryException("unbalanced '{' in '" + s + "'");
        }
        String metric = validateMetricName(s.substring(0, brace).trim());
        String body = s.substring(brace + 1, s.length() - 1);

        List<Matcher> matchers = new ArrayList<>();
        for (String part : splitTopLevel(body, ',')) {
            if (part.isBlank()) {
                continue;
            }
            matchers.add(parseMatcher(part.trim()));
        }
        return new Selector(metric, matchers);
    }

    private static Matcher parseMatcher(String part) {
        // Order matters: two-character operators must be tried first.
        for (String op : new String[] {"=~", "!~", "!=", "="}) {
            int idx = part.indexOf(op);
            if (idx > 0) {
                String label = part.substring(0, idx).trim();
                String value = unquote(part.substring(idx + op.length()).trim());
                Pattern pattern = null;
                if (op.equals("=~") || op.equals("!~")) {
                    try {
                        pattern = Pattern.compile(value);
                    } catch (PatternSyntaxException e) {
                        throw new BadQueryException("bad regex '" + value + "'");
                    }
                }
                return new Matcher(label, op, value, pattern);
            }
        }
        throw new BadQueryException("cannot parse label matcher '" + part + "'");
    }

    private static String unquote(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        throw new BadQueryException("label matcher value must be double-quoted, got " + value);
    }

    private static String validateMetricName(String name) {
        if (!name.matches("[a-zA-Z_:][a-zA-Z0-9_:]*")) {
            throw new BadQueryException("invalid metric name '" + name + "'");
        }
        return name;
    }

    /** Splits on {@code delim}, ignoring occurrences inside double-quoted strings. */
    private static List<String> splitTopLevel(String s, char delim) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' && (i == 0 || s.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
            }
            if (c == delim && !inQuotes) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        parts.add(current.toString());
        return parts;
    }
}
