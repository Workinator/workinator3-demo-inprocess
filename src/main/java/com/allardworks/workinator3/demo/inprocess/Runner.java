package com.allardworks.workinator3.demo.inprocess;

import com.allardworks.workinator3.consumer.WorkinatorConsumer;
import com.allardworks.workinator3.consumer.WorkinatorConsumerFactory;
import com.allardworks.workinator3.core.*;
import com.allardworks.workinator3.core.commands.CreatePartitionCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.experimental.var;
import lombok.val;
import org.apache.commons.cli.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static java.lang.System.lineSeparator;
import static java.lang.System.out;

@Service
@RequiredArgsConstructor
public class Runner implements CommandLineRunner {
    private final Workinator workinator;
    private final WorkinatorConsumerFactory consumerFactory;
    private final Map<String, WorkinatorConsumer> consumers = new HashMap<>();

    /**
     * Create a partition.
     *
     * @param command
     * @throws PartitionExistsException
     */
    private boolean createPartition(final CommandLine command) throws PartitionExistsException {
        val partitionName = command.getOptionValue("cp");
        if (partitionName == null) {
            return false;
        }

        val partition = CreatePartitionCommand
                .builder()
                .partitionKey(partitionName)
                .maxWorkerCount(getWorkerCount(command))
                .build();
        workinator.createPartition(partition);
        return true;
    }

    private int getWorkerCount(final CommandLine command) {
        // i'm pretty sure this is the wrong way to handle command arguments,
        // but i can't figure out the right way.
        val workerCountOption = command.getOptionValue("wc");
        return
                workerCountOption == null
                        ? 1
                        : parseInt(workerCountOption);
    }

    private boolean createConsumer(final CommandLine command) {
        val consumerName = command.getOptionValue("cc");
        if (consumerName == null) {
            return false;
        }

        val id = new ConsumerId(consumerName);
        val configuration = new ConsumerConfiguration();
        configuration.setMaxWorkerCount(getWorkerCount(command));

        val consumer = consumerFactory.create(id, configuration);
        consumer.start();
        consumers.put(consumerName, consumer);
        return true;
    }

    private boolean stopConsumer(final CommandLine command) {
        val consumerName = command.getOptionValue("sc");
        if (consumerName == null) {
            return false;
        }

        val consumer = consumers.get(consumerName);
        if (consumer == null) {
            return false;
        }

        consumer.stop();
        return true;
    }

    /**
     * Shows the status of the consumers running in process.
     *
     * @param command
     * @return
     * @throws JsonProcessingException
     */
    private boolean showLocalConsumerStatus(final CommandLine command) {
        if (!command.hasOption("scl")) {
            return false;
        }

        val output = new StringBuffer();
        for (val c : consumers.values()) {
            output.append("Consumer: " + c.getConsumerId().getName() + lineSeparator());
            output.append("\t" + c.getStatus() + lineSeparator());
            val executors = c.getExecutors();
            for (val e : executors) {
                val assignment =
                        e.getAssignment() == null
                                ? ""
                                : e.getAssignment().getPartitionKey();
                output.append("\t" + e.getWorkerId().getWorkerNumber() + " - " + e.getStatus() + ", Assignment=" + assignment + lineSeparator());
            }
            output.append(lineSeparator());
        }
        out.println(output.toString());
        return true;
    }

    private void showHelp(final Options options) {
        val formatter = new HelpFormatter();
        formatter.printHelp("workinator demo cli", options);
        out.println();
    }

    private boolean showHelp(final CommandLine command, final Options options) {
        if (!command.hasOption("help")) {
            return false;
        }

        showHelp(options);
        return true;
    }

    private boolean showPartitions(final CommandLine command) {
        if (!command.hasOption("sp")) {
            return false;
        }

        val partitions = workinator.getPartitions();
        for (val partition : partitions) {
            out.println("Partition Key=" + partition.getPartitionKey() + ", Max Worker Count=" + partition.getMaxWorkerCount() + ", Last Checked=" + partition.getLastChecked() + ", Current Worker Count=" + partition.getCurrentWorkerCount());
            for (val worker : partition.getWorkers()) {
                out.println("\t" + worker.getAssignee() + ", Rule: " + worker.getRule());
            }
        }
        return true;
    }

    private boolean setPartitionHasWork(final CommandLine command) {
        if (command.hasOption("pwork")) {
            DemoHelper.getHack().setHasWork(command.getOptionValue("pwork"), true);
            out.println("The partition has been set: hasWork=true");
            return true;
        }

        if (command.hasOption("pnwork")) {
            DemoHelper.getHack().setHasWork(command.getOptionValue("pnwork"), false);
            out.println("The partition has been set: hasWork=false");
            return true;
        }

        return false;
    }

    private boolean freezeWorker(final CommandLine command) {
        if (!command.hasOption("wfreeze")) {
            return false;
        }

        DemoHelper.getHack().freezeWorker(command.getOptionValue("wfreeze"));
        out.println("The worker has been set: frozen=true");
        return true;
    }

    private boolean thawWorker(final CommandLine command) {
        if (!command.hasOption("wthaw")) {
            return false;
        }

        DemoHelper.getHack().thawWorker(command.getOptionValue("wthaw"));
        out.println("The worker has been set: frozen=false");
        return true;
    }

    @Override
    public void run(String... strings) {
        val parser = new DefaultParser();

        val options = new Options();
        options.addOption(new Option("cc", "createconsumer", true, "Create a consumer. See also the -wc option."));
        options.addOption(new Option("sc", "stopconsumer", true, "Stop a consumer."));
        options.addOption(new Option("wc", "workercount", true, "For use with -cc. The number of worker threads."));
        options.addOption(new Option("cp", "createpartition", true, "Create a partition"));
        options.addOption(new Option("scl", "showconsumerslocal", false, "Display In Process Consumer Information"));
        options.addOption(new Option("help", "help", false, "print this message"));
        options.addOption(new Option("sp", "showpartitions", false, "show partitions"));
        options.addOption(new Option("pwork", "partitionhaswork", true, "for emulation: indicate that a partition has work."));
        options.addOption(new Option("pnwork", "partitionnowork", true, "for emulation: indicate that a partition doesn't have work."));
        options.addOption(new Option("wfreeze", "workerfreeze", true, "for emulation: freeze a worker. format = consumername.workernumber. IE: a.3 (0 based)"));
        options.addOption(new Option("wthaw", "workerthaw", true, "for emulation: thaw a worker. format = consumername.workernumber. IE: a.3 (0 based)"));
        while (true) {
            try {
                val command = parser.parse(options, getInput());
                val processed =
                        createPartition(command)
                                || createConsumer(command)
                                || stopConsumer(command)
                                || showLocalConsumerStatus(command)
                                || showHelp(command, options)
                                || setPartitionHasWork(command)
                                || showPartitions(command)
                                || freezeWorker(command)
                                || thawWorker(command);

                if (!processed) {
                    showHelp(options);
                }
            } catch (final Exception ex) {
                System.out.println(ex.toString());
                showHelp(options);
            }
        }
    }

    private String[] getInput() {
        try {
            out.print("Workinator> ");
            return new BufferedReader(new InputStreamReader(System.in)).readLine().split(" ");
        } catch (IOException e) {
            return new String[]{};
        }
    }
}