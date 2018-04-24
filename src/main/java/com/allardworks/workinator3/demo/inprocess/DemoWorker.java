package com.allardworks.workinator3.demo.inprocess;

import com.allardworks.workinator3.consumer.AsyncWorker;
import com.allardworks.workinator3.consumer.WorkerContext;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.io.Console;

@RequiredArgsConstructor
public class DemoWorker implements AsyncWorker {
    private boolean lastHadWork = false;
    @Override
    public void execute(WorkerContext context) {
        val hasWork = DemoHelper.getHack().getPartitionHasWork(context.getAssignment().getPartitionKey());
        context.hasWork(hasWork);
        if (hasWork != lastHadWork) {
            System.out.println("Has work change: Partition " + context.getAssignment().getPartitionKey() + " = " + hasWork);
            lastHadWork = hasWork;
        }
        while (DemoHelper.getHack().getWorkerIsFrozen(context.getAssignment().getWorkerId().getConsumer().getConsumerId().getName(), context.getAssignment().getWorkerId().getWorkerNumber())) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
    }
}