/*
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.result.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author tim.lessner@freiheit.com
 */
class DirectoryFileFetcher<OriginalInput> implements Fetcher<OriginalInput> {
    private static final Logger LOG = LoggerFactory.getLogger( DirectoryFileFetcher.class );

    private static final class FetchFile<OriginalInput> implements Function<Path, Result<FetchedItem<OriginalInput>, OriginalInput>> {
        private final Function<Path, OriginalInput> func;
        private int counter = FetchedItem.FIRST_ROW;

        public FetchFile( final Function<Path, OriginalInput> func ) {
            this.func = func;
        }

        @Override
        public Result<FetchedItem<OriginalInput>, OriginalInput> apply( final Path path ) {
            final int rownum = counter;
            counter++;
            try {
                final OriginalInput r = func.apply( path );
                final FetchedItem<OriginalInput> fetchedItem = FetchedItem.<OriginalInput> of( r, rownum, path.toString() );
                return Result.success( fetchedItem, r );
            } catch ( final Throwable t ) {
                final FetchedItem<OriginalInput> fetchedItem = FetchedItem.<OriginalInput> of( null, rownum, path.toString() );
                return Result.failed( fetchedItem, "Failed to read from " + path, t );
            }
        }
    }

    public static final Comparator<Path> ORDERING_FILE_BY_PATH = Comparator.comparing( path -> path.getFileName().toString(), Comparator.naturalOrder() );
    private final List<DownloadDir> dirs;
    private final Function<Path, OriginalInput> func;
    private final Comparator<Path> fileOrdering;

    public DirectoryFileFetcher( final Path uri, final String filter, final Function<Path, OriginalInput> func ) {
        this( func, ORDERING_FILE_BY_PATH, new DownloadDir( uri, null, filter ) );
    }

    public DirectoryFileFetcher( 
            final Function<Path, OriginalInput> func,
            final Comparator<Path> fileOrdering,
            final DownloadDir dir,
            final DownloadDir... dirs ) {
        this( func, fileOrdering, mergeDownloadDirs( dir, dirs ) );
    }

    private static List<DownloadDir> mergeDownloadDirs( final DownloadDir dir, final DownloadDir[] dirs ) {
        final List<DownloadDir> results = new ArrayList<>( dirs.length + 1 );
        results.add( dir );
        Collections.addAll( results, dirs );
        return Collections.unmodifiableList( results );
    }

    public DirectoryFileFetcher( 
            final Function<Path, OriginalInput> func,
            final Comparator<Path> fileOrdering,
            final List<DownloadDir> dirs ) {
        this.fileOrdering = Objects.requireNonNull( fileOrdering );
        this.dirs = dirs;
        this.func = Objects.requireNonNull( func );
    }


    @Override
    public Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> fetchAll() {
        final FetchFile<OriginalInput> resultCreator = new FetchFile<>( func );
        // the directories are all read eagerly, so we copy the concatenated iterable into a list
        // The caller (BatchJob) may iterate over Collections to collect statistics, but it will not 
        // iterate over Iterables to not break lazily loaded data. Thus we will copy the iterable into a list to preserve that.

        final List<Result<FetchedItem<OriginalInput>, OriginalInput>> iter = dirs.stream()
                .map( dir -> {
                    try {
                        if ( !Files.exists( dir.getPath() ) ) {
                            return Collections.<Result<FetchedItem<OriginalInput>, OriginalInput>>emptyList();
                        }
                        return fetchFromDirectory( dir.getPath(), resultCreator, dir.getPrefix(), dir.getSuffix() );
                    } catch ( final IOException e ) {
                        List<Result<FetchedItem<OriginalInput>, OriginalInput>> result = Collections.singletonList( Result.failed( FetchedItem.of( null, 0, dirs.toString() ), "Could not traverse download directory" ) );
                        return result;
                    }
                } )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );

        int counter = 0;
        final List<Result<FetchedItem<OriginalInput>, OriginalInput>> b = new ArrayList<>();
        for ( final Result<FetchedItem<OriginalInput>, OriginalInput> r : iter ) {
            b.add( r );
            counter++;
            if ( (counter % 100) == 0) {
                LOG.info( "fetched " + counter + " / ? files" );
            }
        }
        LOG.info( "Finished: Directory Fetcher fetched " + counter + " files" );
        return Collections.unmodifiableList( b );
    }

    private List<Result<FetchedItem<OriginalInput>, OriginalInput>> fetchFromDirectory(
            final Path dir,
            final java.util.function.Function<Path, Result<FetchedItem<OriginalInput>, OriginalInput>> resultCreator,
            final String prefix,
            final String suffix
    ) throws IOException {
        try ( final Stream<Path> files = Files.walk( dir ) ) {
            return files
                    .filter( path -> {
                        final String name = path.getFileName().toString();
                        if ( !(prefix == null || prefix.isEmpty()) && !name.startsWith( prefix ) ) {
                            return false;
                        }
                        return name.endsWith( suffix );
                    } )
                    .sorted( fileOrdering )
                    .map( resultCreator )
                    .collect( Collectors.toList() );
        }
    }
}
