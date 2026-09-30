package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.CliContext;
import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.cli.format.TableFormatter;
import io.github.pandeyayushk.jobstream.worker.WorkerInfo;
import io.github.pandeyayushk.jobstream.worker.WorkerRegistry;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "worker",
        description = "Inspect registered JobStream workers.",
        mixinStandardHelpOptions = true,
        subcommands = {
                WorkerCommand.ListCommand.class
        }
)
public final class WorkerCommand implements Runnable {

    @ParentCommand
    private JobStreamCli root;

    CliContext context() {
        return root.context();
    }

    @Override
    public void run() {
        System.out.println(
                "Use 'jobstream worker --help' for available commands."
        );
    }

    @Command(
            name = "list",
            description = "List currently registered workers.",
            mixinStandardHelpOptions = true
    )
    public static final class ListCommand implements Callable<Integer> {

        @ParentCommand
        private WorkerCommand parent;

        @Override
        public Integer call() {
            WorkerRegistry registry = parent.context().workerRegistry();
            List<WorkerInfo> workers = registry.listActiveWorkers();

            if (workers.isEmpty()) {
                System.out.println("No registered workers.");
                return 0;
            }

            List<List<String>> rows = new ArrayList<>();
            for (WorkerInfo worker : workers) {
                rows.add(List.of(
                        worker.workerId().toString(),
                        worker.status().name(),
                        worker.startedAt().toString()
                ));
            }

            System.out.print(
                    TableFormatter.format(
                            List.of("WORKER ID", "STATUS", "STARTED AT"),
                            rows
                    )
            );
            return 0;
        }
    }
}
