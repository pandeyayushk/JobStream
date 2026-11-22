package io.github.pandeyayushk.jobstream.cli;

import io.github.pandeyayushk.jobstream.cli.command.DlqCommand;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/** Runs the command shortcuts for one interactive terminal session. */
public final class InteractiveShell {

    private static final String DEFAULT_QUEUE = "default";

    private final JobStreamCli root;
    private final BufferedReader input;
    private final PrintStream output;
    private final CommandLine commandLine;

    public InteractiveShell(JobStreamCli root) {
        this(root, new BufferedReader(new java.io.InputStreamReader(System.in)), System.out);
    }

    InteractiveShell(JobStreamCli root, BufferedReader input, PrintStream output) {
        this.root = root;
        this.input = input;
        this.output = output;
        this.commandLine = JobStreamCli.createCommandLine(root);
        this.commandLine.setOut(new PrintWriter(output, true));
        this.commandLine.setErr(new PrintWriter(output, true));
    }

    /** Starts the prompt loop and closes the shared CLI context exactly once. */
    public void run() {
        try {
            printBanner();
            try {
                root.context().jobQueue().size(DEFAULT_QUEUE);
                output.println("Connected to Redis at " + root.getHost() + ":" + root.getPort());
            } catch (RuntimeException e) {
                output.println("Redis connection error: " + CliExecutionExceptionHandler.userMessage(e));
                output.println("The shell will remain open; Redis commands can be retried.");
            }
            output.println("Type 'help' to see available commands.");

            String line;
            while (true) {
                output.print("js> ");
                output.flush();
                try {
                    line = input.readLine();
                } catch (IOException e) {
                    output.println();
                    output.println("Input ended; closing shell.");
                    break;
                }
                if (line == null) {
                    output.println();
                    break;
                }
                if (!executeLine(line)) {
                    break;
                }
            }
        } finally {
            root.close();
        }
    }

    private void printBanner() {
        output.println("========================================");
        output.println("               JOBSTREAM");
        output.println("          Distributed Job Queue");
        output.println("========================================");
    }

    /** Executes one shell line. Returns false when the shell should exit. */
    boolean executeLine(String line) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return true;
        }

        String command = firstToken(trimmed).toLowerCase();
        if (command.equals("exit") || command.equals("quit")) {
            output.println("Goodbye.");
            return false;
        }
        if (command.equals("help")) {
            printHelp();
            return true;
        }
        if (command.equals("clear")) {
            clearScreen();
            return true;
        }

        try {
            List<String> args = toPicocliArguments(trimmed);
            if (args == null) {
                return true;
            }
            if (command.equals("queue") && tokenize(trimmed).size() == 2) {
                executeCommand(args);
                executeCommand(List.of("queue", "peek", args.get(2)));
                return true;
            }
            preparePurgeInput(args);
            executeCommand(args);
        } catch (IllegalArgumentException e) {
            output.println("Error: " + e.getMessage());
        } catch (RuntimeException e) {
            output.println("Error: " + CliExecutionExceptionHandler.userMessage(e));
        }
        return true;
    }

    private List<String> toPicocliArguments(String line) {
        List<String> words = tokenize(line);
        if (words.isEmpty()) {
            return List.of();
        }
        String command = words.get(0).toLowerCase();
        return switch (command) {
            case "submit" -> submitArguments(line, words);
            case "status" -> statusArguments(words);
            case "queue" -> queueArguments(words);
            case "workers" -> requireArity(words, 1, List.of("worker", "list"));
            case "dlq" -> requireArity(words, 1, List.of("dlq", "list"));
            case "requeue" -> requeueArguments(words);
            case "purge" -> requireArity(words, 1, List.of("dlq", "purge"));
            default -> {
                output.println("Unknown command: " + words.get(0) + ". Type 'help' for available commands.");
                yield null;
            }
        };
    }

    private List<String> submitArguments(String line, List<String> words) {
        if (words.size() < 3) {
            throw new IllegalArgumentException("Usage: submit <type> <json>");
        }
        int commandEnd = skipToken(line, skipWhitespace(line, 0));
        int actualTypeStart = skipWhitespace(line, commandEnd);
        int typeEnd = skipToken(line, actualTypeStart);
        int jsonStart = skipWhitespace(line, typeEnd);
        String type = line.substring(actualTypeStart, typeEnd);
        String json = line.substring(jsonStart).trim();
        if (json.isEmpty()) {
            throw new IllegalArgumentException("Usage: submit <type> <json>");
        }
        return List.of("job", "submit", "--type", type, "--payload", json);
    }

    private List<String> queueArguments(List<String> words) {
        if (words.size() > 2) {
            throw new IllegalArgumentException("Usage: queue [<name>]");
        }
        String queue = words.size() == 1 ? DEFAULT_QUEUE : words.get(1);
        return List.of("queue", "size", queue);
    }

    private List<String> statusArguments(List<String> words) {
        if (words.size() != 2) {
            throw new IllegalArgumentException("Usage: status <id>");
        }
        return List.of("job", "status", words.get(1));
    }

    private List<String> requeueArguments(List<String> words) {
        if (words.size() < 2 || words.size() > 3) {
            throw new IllegalArgumentException("Usage: requeue <id> [<queue>]");
        }
        String queue = words.size() == 3 ? words.get(2) : DEFAULT_QUEUE;
        return List.of("dlq", "requeue", words.get(1), "--target-queue", queue);
    }

    private List<String> requireArity(List<String> words, int count, List<String> args) {
        if (words.size() != count) {
            throw new IllegalArgumentException("Unexpected arguments: " + String.join(" ", words.subList(1, words.size())));
        }
        return args;
    }

    private void preparePurgeInput(List<String> args) {
        if (args.size() != 2 || !args.get(0).equals("dlq") || !args.get(1).equals("purge")) {
            return;
        }
        CommandLine dlq = commandLine.getSubcommands().get("dlq");
        DlqCommand.PurgeCommand purge = (DlqCommand.PurgeCommand) dlq.getSubcommands().get("purge").getCommand();
        purge.setConfirmationInput(input);
    }

    private void executeCommand(List<String> args) {
        int exitCode = commandLine.execute(args.toArray(String[]::new));
        if (exitCode != 0) {
            output.println("Command failed with exit code " + exitCode + ".");
        }
    }

    private void printHelp() {
        output.println("Available commands:");
        output.println("  help                         Show this help");
        output.println("  submit <type> <json>         Submit a job to the default queue");
        output.println("  status <id>                  Show job details");
        output.println("  queue [<name>]                Show queue size; named queues also show IDs");
        output.println("  workers                       List active workers");
        output.println("  dlq                           List dead-letter jobs");
        output.println("  requeue <id> [<queue>]        Requeue a dead job");
        output.println("  purge                         Purge the DLQ with confirmation");
        output.println("  clear                         Clear terminal output where supported");
        output.println("  exit, quit                    Close the shell");
        output.println("JSON payloads may contain spaces, nested objects, and arrays.");
    }

    private void clearScreen() {
        if (System.console() != null || System.getenv("TERM") != null) {
            output.print("\u001b[2J\u001b[H");
        } else {
            for (int i = 0; i < 30; i++) {
                output.println();
            }
        }
        output.flush();
    }

    private static String firstToken(String line) {
        int end = skipToken(line, skipWhitespace(line, 0));
        return end == 0 ? "" : line.substring(skipWhitespace(line, 0), end);
    }

    /** Splits on whitespace outside quoted strings, respecting JSON escapes. */
    static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;
        int jsonNesting = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (escaped) {
                token.append(c);
                escaped = false;
            } else if (c == '\\' && inQuotes) {
                token.append(c);
                escaped = true;
            } else if (c == '"') {
                token.append(c);
                inQuotes = !inQuotes;
            } else if (!inQuotes && (c == '{' || c == '[')) {
                jsonNesting++;
                token.append(c);
            } else if (!inQuotes && (c == '}' || c == ']')) {
                jsonNesting--;
                token.append(c);
            } else if (Character.isWhitespace(c) && !inQuotes && jsonNesting <= 0) {
                if (!token.isEmpty()) {
                    tokens.add(token.toString());
                    token.setLength(0);
                }
            } else {
                token.append(c);
            }
        }
        if (inQuotes) {
            throw new IllegalArgumentException("Unclosed quotation mark.");
        }
        if (!token.isEmpty()) {
            tokens.add(token.toString());
        }
        return tokens;
    }

    private static int skipWhitespace(String input, int index) {
        while (index < input.length() && Character.isWhitespace(input.charAt(index))) {
            index++;
        }
        return index;
    }

    private static int skipToken(String input, int index) {
        while (index < input.length() && !Character.isWhitespace(input.charAt(index))) {
            index++;
        }
        return index;
    }
}
