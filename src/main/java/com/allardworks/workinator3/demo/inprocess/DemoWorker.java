package com.allardworks.workinator3.demo.inprocess;

import com.allardworks.workinator3.consumer.AsyncWorker;
import com.allardworks.workinator3.consumer.WorkerContext;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DemoWorker implements AsyncWorker {
    @Override
    public void execute(WorkerContext context) {
        context.hasWork(DemoHelper.getHack().getPartitionHasWork(context.getAssignment().getPartitionKey()));
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