package org.pogonin.mapReduce;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;


class Coordinator extends Thread {

    private final List<String> finishResult;
    private final ExecutorService executor;
    private CountDownLatch countDownLatch;
    private final List<Task> reduceTasks;
    private final List<String> mapResult;
    private final List<Task> mapTasks;
    private final String directory;
    private final int taskCount;
    private int taskIdCounter;

    Coordinator(ExecutorService executor, List<String> listTaskPaths, String directory) {
        taskCount = listTaskPaths.size();

        countDownLatch = new CountDownLatch(taskCount);
        finishResult = new ArrayList<>();
        reduceTasks = new ArrayList<>();
        mapResult = new ArrayList<>();
        this.directory = directory;
        this.executor = executor;

        mapTasks = parseMapTask(listTaskPaths);
    }

    @Override
    public void run() {
            try {
                mapTasks.forEach(t -> executor.execute(new Worker(this, t, taskCount)));

                countDownLatch.await();
                countDownLatch = new CountDownLatch(taskCount);
                reduceTasks.addAll(parseReduceTask());
                reduceTasks.forEach(t -> executor.execute(new Worker(this, t, taskCount)));

                countDownLatch.await();
                finish();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                executor.shutdown();
            }
    }


    private List<Task> parseMapTask(List<String> mapTasks) {
        return mapTasks.stream()
                .map(s -> new Task(taskIdCounter++, new Path[]{Path.of(directory + s)}, Task.TaskType.map)).toList();
    }

    private List<Task> parseReduceTask() {
        return mapResult.stream()
                .collect(Collectors.groupingBy(s -> s.substring(s.lastIndexOf("-") + 1)))
                .values().stream()
                .map(strings -> new Task(
                        taskIdCounter++,
                        strings.stream().map(Path::of).toArray(Path[]::new),
                        Task.TaskType.reduce))
                .toList();
    }

    private void finish() {
        TreeMap<String, Long> map = new TreeMap<>();
        for (String s : finishResult) {
            try(BufferedReader reader = new BufferedReader(new FileReader(s))) {
                while(reader.ready()) {
                    String[] line = reader.readLine().split(":");
                    map.put(line[0], Long.parseLong(line[1]));
                }
                reader.close();
                Files.delete(Path.of(s));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(directory + "result"))) {
            for(Map.Entry<String, Long> entry : map.entrySet()) {
                writer.write(entry.getKey() + ":" + entry.getValue() + "\n");
            }
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    File getFile(String mapTaskId, String reduceTaskId) {
        return new File(directory + File.separator + "mr-" + mapTaskId + "-" + reduceTaskId);
    }

    void finishMapTask(List<String> reduceTask) {
        mapResult.addAll(reduceTask);
        countDownLatch.countDown();
    }

    void finishReduceTask(String finishPath) {
        finishResult.add(finishPath);
        countDownLatch.countDown();
    }

}
