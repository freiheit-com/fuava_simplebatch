package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;

public class FileUtil {

    public static Path getLogFilePath(final Path dataFilePath, final String fileEnding) {
        return dataFilePath.resolveSibling( dataFilePath.getFileName().toString() + fileEnding );
    }

    public static Path getControlFilePath(final Path dataFilePath, final String fileEnding) {
        return dataFilePath.resolveSibling( dataFilePath.getFileName().toString() + fileEnding );
    }

}
