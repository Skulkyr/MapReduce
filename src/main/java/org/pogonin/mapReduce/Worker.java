package org.pogonin.mapReduce;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;


public class Worker implements Runnable {
    Coordinator coordinator;
    Task task;
    int workerCount;

    Worker(Coordinator coordinator, Task task, int workerCount) {
        this.coordinator = coordinator;
        this.task = task;
        this.workerCount = workerCount;
    }

    @Override
    public void run() {
        if (task.taskType == Task.TaskType.map) map(task);
        else if (task.taskType == Task.TaskType.reduce) reduce(task);
    }

    private void map(Task task) {
        List<Map<String, Integer>> resultMap = new ArrayList<>();
        String[] words = new String[0];

        for (Path path : task.filePaths) {
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                words = reader.lines()
                        .map(s -> s.replaceAll("\\p{Punct}", ""))
                        .flatMap(s -> Arrays.stream(s.split("\\s")))
                        .toArray(String[]::new);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < workerCount; i++)
            resultMap.add(new HashMap<>());

        for (String s : words) {
            if (s.isBlank()) continue;
            int bucketNumber = getBucketNumber(s);
            if (resultMap.get(bucketNumber).containsKey(s))
                resultMap.get(bucketNumber).put(s, resultMap.get(bucketNumber).get(s) + 1);
            else resultMap.get(bucketNumber).put(s, 1);
        }

        List<String> mapResult = mapResultWriteToFile(resultMap, String.valueOf(task.id));
        coordinator.finishMapTask(mapResult);
    }

    private List<String> mapResultWriteToFile(List<Map<String, Integer>> mapResult, String taskId) {
        List<String> resultList = new ArrayList<>();
        for (int i = 0; i < mapResult.size(); i++) {
            File file = coordinator.getFile(taskId, String.valueOf(i));
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
                for (Map.Entry<String, Integer> entry : mapResult.get(i).entrySet()) {
                    writer.write(String.format("%s:%s\n", entry.getKey(), entry.getValue()));
                    writer.flush();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            resultList.add(file.getAbsolutePath());
        }
        return resultList;
    }

    private static int getBucketNumber(String input) {
        return (Math.abs(input.hashCode() << 2) % 3);
    }

    private void reduce(Task task) {
        Map<String, Integer> resultMap = new HashMap<>();
        for (int i = 0; i < task.filePaths.length; i++) {
            try (BufferedReader reader = Files.newBufferedReader(task.filePaths[i])) {
                while (reader.ready()) {
                    String[] keyVal = reader.readLine().split(":");
                    if (resultMap.containsKey(keyVal[0]))
                        resultMap.put(keyVal[0], resultMap.get(keyVal[0]) + Integer.parseInt(keyVal[1]));
                    else resultMap.put(keyVal[0], Integer.parseInt(keyVal[1]));
                }
                reader.close();
                Files.delete(task.filePaths[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("G:/test/result-" + task.id, true))) {
            for (Map.Entry<String, Integer> entry : resultMap.entrySet())
                bufferedWriter.write(String.format("%s:%s\n", entry.getKey(), entry.getValue()));
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        coordinator.finishReduceTask("G:/test/result-" + task.id);
    }
}
