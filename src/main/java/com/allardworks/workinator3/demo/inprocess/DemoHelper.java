package com.allardworks.workinator3.demo.inprocess;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jaya on 3/7/18.
 *
 * HACKETY HACK HACK
 *
 * The DEMO app hosts multiple consumersin a single process.
 * The single instance of this class is used by each of the workers.
 * It allows the demo app to set values that control the workers behavior.
 *
 * This is a wicked hack. It only works for consumers that are in process (which
 * is only a valid scenario for test purposes), and it exposes a singleton.
 * Don't do this at home. It's only for use by the demo application.
 * Workinator will eventually have a thin messaging layer for communication with workers.
 * This will go away once that is in place.
 */
public class DemoHelper {
    private final static DemoHelper hack = new DemoHelper();

    public static DemoHelper getHack() {
        return hack;
    }

    @Data
    private class PartitionEmulatorSettings {
        public boolean hasWork;
    }

    @Data
    private class WorkerEmulatorSettings {
        public boolean frozen;
    }

    private final Map<String, PartitionEmulatorSettings> partitions = new HashMap<>();
    private final Map<String, WorkerEmulatorSettings> workers = new HashMap<>();

    private PartitionEmulatorSettings getPartition(final String partitionKey) {
        return partitions.computeIfAbsent(partitionKey, pk -> new PartitionEmulatorSettings());
    }

    private WorkerEmulatorSettings getWorker(final String workerAlias) {
        return workers.computeIfAbsent(workerAlias, a -> new WorkerEmulatorSettings());
    }

    public DemoHelper setHasWork(final String partitionKey, final boolean hasWork) {
        getPartition(partitionKey).setHasWork(hasWork);
        return this;
    }

    public DemoHelper freezeWorker(final String alias) {
        getWorker(alias).setFrozen(true);
        return this;
    }

    public DemoHelper thawWorker(final String alias) {
        getWorker(alias).setFrozen(false);
        return this;
    }
    public boolean getPartitionHasWork(final String partitionKey){
        return getPartition(partitionKey).isHasWork();
    }

    public boolean getWorkerIsFrozen(final String consumerName, final int workerNumber) {
        return getWorker(consumerName + "." + workerNumber).isFrozen();
    }

    public DemoHelper clear() {
        partitions.clear();
        return this;
    }
}
