package io.github.pandeyayushk.jobstream.cli;

import picocli.CommandLine;
import picocli.CommandLine.IParameterExceptionHandler;
import picocli.CommandLine.ParameterException;

final class CliParameterExceptionHandler implements IParameterExceptionHandler {

    @Override
    public int handleParseException(ParameterException ex, String[] args) {
        CommandLine commandLine = ex.getCommandLine();
        commandLine.getErr().println("Error: " + ex.getMessage());
        commandLine.usage(commandLine.getErr());
        return commandLine.getCommandSpec().exitCodeOnInvalidInput();
    }
}
