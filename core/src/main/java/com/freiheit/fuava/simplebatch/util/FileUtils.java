package com.freiheit.fuava.simplebatch.util;

import com.google.common.base.Strings;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class FileUtils {

    public static void deleteDirectoryRecursively( final File dir ) throws IOException {
        Files.walkFileTree( Paths.get( dir.toURI() ), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile( final Path file, final BasicFileAttributes attrs ) throws IOException {
                Files.delete( file );
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory( final Path dir, final IOException exc ) throws IOException {
                Files.delete( dir );
                return FileVisitResult.CONTINUE;
            }

        } );
    }

    public static String getCurrentDateDirPath( final String path  ) {
        final String dateDirString = LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE );
        if ( Strings.isNullOrEmpty( path ) ) {
            return dateDirString;
        }
        return path.endsWith( File.separator )
            ? path + dateDirString + File.separator
            : path + File.separator + dateDirString + File.separator;
    }
}
