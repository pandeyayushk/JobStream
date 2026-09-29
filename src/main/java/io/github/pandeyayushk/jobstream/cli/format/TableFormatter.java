package io.github.pandeyayushk.jobstream.cli.format;

import java.util.List;
import java.util.Objects;

public final class TableFormatter {

    private TableFormatter() {
    }

    public static String format(List<String> headers, List<List<String>> rows) {
        Objects.requireNonNull(headers, "headers must not be null");
        Objects.requireNonNull(rows, "rows must not be null");

        if (headers.isEmpty()) {
            return "";
        }

        int columnCount = headers.size();

        for (List<String> row : rows) {
            if (row == null || row.size() != columnCount) {
                throw new IllegalArgumentException(
                        "Each row must contain exactly " + columnCount + " columns"
                );
            }
        }

        int[] columnWidths = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
            columnWidths[i] = safeValue(headers.get(i)).length();
        }

        for (List<String> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                columnWidths[i] = Math.max(
                        columnWidths[i],
                        safeValue(row.get(i)).length()
                );
            }
        }

        StringBuilder output = new StringBuilder();

        appendRow(output, headers, columnWidths);

        if (!rows.isEmpty()) {
            appendSeparator(output, columnWidths);

            for (List<String> row : rows) {
                appendRow(output, row, columnWidths);
            }
        }

        return output.toString();
    }

    private static void appendRow(
            StringBuilder output,
            List<String> values,
            int[] columnWidths
    ) {
        for (int i = 0; i < columnWidths.length; i++) {
            if (i > 0) {
                output.append("  ");
            }

            String value = safeValue(values.get(i));

            output.append(value);

            int padding = columnWidths[i] - value.length();

            output.append(" ".repeat(padding));
        }

        output.append(System.lineSeparator());
    }

    private static void appendSeparator(
            StringBuilder output,
            int[] columnWidths
    ) {
        for (int i = 0; i < columnWidths.length; i++) {
            if (i > 0) {
                output.append("  ");
            }

            output.append("-".repeat(columnWidths[i]));
        }

        output.append(System.lineSeparator());
    }

    private static String safeValue(String value) {
        return value == null ? "" : value;
    }
}