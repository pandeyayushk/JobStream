package io.github.pandeyayushk.jobstream.cli.format;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TableFormatterTest {

    @Test
    void formatsNormalTable() {
        List<String> headers = List.of("ID", "STATUS", "RETRIES");

        List<List<String>> rows = List.of(
                List.of("abc-123", "COMPLETED", "0"),
                List.of("def-456", "FAILED", "2")
        );

        String result = TableFormatter.format(headers, rows);

        String expected = String.join(
                System.lineSeparator(),
                "ID       STATUS     RETRIES",
                "-------  ---------  -------",
                "abc-123  COMPLETED  0      ",
                "def-456  FAILED     2      "
        ) + System.lineSeparator();

        assertEquals(expected, result);
    }

    @Test
    void adjustsColumnWidthsForLongValues() {
        List<String> headers = List.of("ID", "STATUS");

        List<List<String>> rows = List.of(
                List.of("very-long-job-id", "COMPLETED"),
                List.of("short", "FAILED")
        );

        String result = TableFormatter.format(headers, rows);

        String expected = String.join(
                System.lineSeparator(),
                "ID                STATUS   ",
                "----------------  ---------",
                "very-long-job-id  COMPLETED",
                "short             FAILED   "
        ) + System.lineSeparator();

        assertEquals(expected, result);
    }

    @Test
    void formatsHeaderWhenRowsAreEmpty() {
        List<String> headers = List.of("ID", "STATUS");

        String result = TableFormatter.format(headers, List.of());

        String expected = "ID  STATUS" + System.lineSeparator();

        assertEquals(expected, result);
    }

    @Test
    void formatsSingleColumnTable() {
        List<String> headers = List.of("ID");

        List<List<String>> rows = List.of(
                List.of("abc-123"),
                List.of("def-456")
        );

        String result = TableFormatter.format(headers, rows);

        String expected = String.join(
                System.lineSeparator(),
                "ID     ",
                "-------",
                "abc-123",
                "def-456"
        ) + System.lineSeparator();

        assertEquals(expected, result);
    }

    @Test
    void rejectsRowsWithIncorrectColumnCount() {
        List<String> headers = List.of("ID", "STATUS");

        List<List<String>> rows = List.of(
                List.of("abc-123")
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> TableFormatter.format(headers, rows)
        );
    }

    @Test
    void handlesNullCellValues() {
        List<String> headers = List.of("ID", "STATUS");

        List<List<String>> rows = List.of(
                Arrays.asList(null, "FAILED")
        );

        String result = TableFormatter.format(headers, rows);

        String expected = String.join(
                System.lineSeparator(),
                "ID  STATUS",
                "--  ------",
                "    FAILED"
        ) + System.lineSeparator();

        assertEquals(expected, result);
    }
}