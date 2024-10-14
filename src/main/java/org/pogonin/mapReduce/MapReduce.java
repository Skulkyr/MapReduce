package org.pogonin.mapReduce;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {
    private final ExecutorService executor;

    public MapReduce(int workerCount) {
        executor = Executors.newFixedThreadPool(workerCount);
    }

    public void execute(List<String> tasks, String directory) {
        new Coordinator(executor, tasks, directory).start();
    }
}
