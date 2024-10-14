package org.pogonin.mapReduce;

import lombok.AllArgsConstructor;

import java.nio.file.Path;


@AllArgsConstructor
public class Task {
    final long id;
    final Path[] filePaths;
    final TaskType taskType;


    public enum TaskType {
        map, reduce
    }
}
