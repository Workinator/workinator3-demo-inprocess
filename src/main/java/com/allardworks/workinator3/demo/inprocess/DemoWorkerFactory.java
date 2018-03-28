package com.allardworks.workinator3.demo.inprocess;

import com.allardworks.workinator3.consumer.AsyncWorker;
import com.allardworks.workinator3.consumer.AsyncWorkerFactory;
import com.allardworks.workinator3.core.Assignment;
import org.springframework.stereotype.Component;

@Component
public class DemoWorkerFactory implements AsyncWorkerFactory {
    @Override
    public AsyncWorker createWorker(Assignment assignment) {
        return new DemoWorker();
    }
}
