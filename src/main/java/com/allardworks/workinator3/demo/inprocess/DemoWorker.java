package com.allardworks.workinator3.demo.inprocess;

import com.allardworks.workinator3.consumer.AsyncWorker;
import com.allardworks.workinator3.consumer.WorkerContext;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.io.Console;

public class DemoWorker implements AsyncWorker {
    //public DemoWorker() {
    //    System.out.println("created demo worker");
    //}

    @Override
    public void execute(WorkerContext context) {
        // determine if the partition has work. for demo purposes, this is set by the test console.
        // it is set in a static variable this is retrieved here.
        // later, this can be handled through messaging.
        val hasWork = DemoHelper.getHack().getPartitionHasWork(context.getAssignment().getPartitionKey());
        context.hasWork(hasWork);


        // same with ISFROZEN. if the static variable is set to frozen, this loop
        // keeps going. this emulates a worker that's blocking.
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