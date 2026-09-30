package io.github.pandeyayushk.jobstream.cli;

import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;
import redis.clients.jedis.exceptions.JedisException;

final class CliExecutionExceptionHandler implements IExecutionExceptionHandler {

    @Override
    public int handleExecutionException(
            Exception ex,
            CommandLine commandLine,
            ParseResult parseResult
    ) {
        commandLine.getErr().println("Error: " + userMessage(ex));
        return commandLine.getCommandSpec().exitCodeOnExecutionException();
    }

    static String userMessage(Throwable throwable) {
        if (isRedisFailure(throwable)) {
            String detail = firstMessage(throwable);
            if (detail == null || detail.isBlank()) {
                return "Unable to connect to Redis.";
            }
            return "Unable to connect to Redis: " + detail;
        }

        String message = firstMessage(throwable);
        if (message == null || message.isBlank()) {
            return "Command failed.";
        }
        return message;
    }

    private static boolean isRedisFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof JedisException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String firstMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null && !current.getMessage().isBlank()) {
                return current.getMessage();
            }
            current = current.getCause();
        }
        return null;
    }
}
