/**
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.EagernessUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

/**
 * @author tim.lessner@freiheit.com
 */
class DirectoryFileFetcher<T> implements Fetcher<T> {

    private static final class FetchFile<T> implements java.util.function.Function<Path, Result<FetchedItem<T>, T>> {
        private final Function<Path, T> func;
        private int counter = FetchedItem.FIRST_ROW;

        public FetchFile( final Function<Path, T> func ) {
            this.func = func;
        }

        @Override
        public Result<FetchedItem<T>, T> apply( final Path path ) {
            final int rownum = counter;
            counter++;
            try {
                final T r = func.apply( path );
                final FetchedItem<T> fetchedItem = FetchedItem.<T> of( r, rownum, path.toString() );
                return Result.success( fetchedItem, r );
            } catch ( final Throwable t ) {
                final FetchedItem<T> fetchedItem = FetchedItem.<T> of( null, rownum, path.toString() );
                return Result.failed( fetchedItem, "Failed to read from " + path, t );
            }
        }
    }

    public static final Ordering<Path> ORDERING_FILE_BY_PATH = Ordering.natural().onResultOf( path -> path.getFileName().toString() );
    private final List<DownloadDir> dirs;
    private final Function<Path, T> func;
    private final Ordering<Path> fileOrdering;

    public DirectoryFileFetcher( final Path uri, final String filter, final Function<Path, T> func ) {
        this( func, ORDERING_FILE_BY_PATH, new DownloadDir( uri, null, filter ) );
    }

    public DirectoryFileFetcher( 
            final Function<Path, T> func,
            final Ordering<Path> fileOrdering, 
            final DownloadDir dir,
            final DownloadDir... dirs ) {
        this( func, fileOrdering, ImmutableList.<DownloadDir>builder().add( dir ).addAll( ImmutableList.copyOf( dirs ) ).build() );
    }

    public DirectoryFileFetcher( 
            final Function<Path, T> func,
            final Ordering<Path> fileOrdering, 
            final List<DownloadDir> dirs ) {
        this.fileOrdering = Preconditions.checkNotNull( fileOrdering );
        this.dirs = dirs;
        this.func = Preconditions.checkNotNull( func );
    }


    @Override
    public Iterable<Result<FetchedItem<T>, T>> fetchAll() {
        final FetchFile<T> resultCreator = new FetchFile<>( func );
        // the directories are all read eagerly, so we copy the concatenated iterable into a list
        // The caller (BatchJob) may iterate over Collections to collect statistics, but it will not 
        // iterate over Iterables to not break lazily loaded data
        return EagernessUtil.preserveEagerness( Iterables.concat( Lists.<DownloadDir, Iterable<Result<FetchedItem<T>, T>>>transform( 
                dirs, 
                dir -> {
                    try {
                        if ( !Files.exists( dir.getPath() ) ) {
                            return ImmutableList.of();
                        }
                        return fetchFromDirectory( dir.getPath(), resultCreator, dir.getPrefix(), dir.getSuffix() );
                    } catch ( final IOException e ) {
                        return ImmutableList.of( Result.failed( FetchedItem.of( null, 0, dirs.toString() ), "Could not traverse download directory") );
                    }
                }
                ) ) 
                );

    }

    private Iterable<Result<FetchedItem<T>, T>> fetchFromDirectory(
            final Path dir,
            final java.util.function.Function<Path, Result<FetchedItem<T>, T>> resultCreator,
            final String prefix,
            final String suffix
    ) throws IOException {
        try ( final Stream<Path> files = Files.walk( dir ) ) {
            final List<Result<FetchedItem<T>, T>> result = files
                    .filter( path -> {
                        final String name = path.getFileName().toString();
                        if ( !Strings.isNullOrEmpty( prefix ) && !name.startsWith( prefix ) ) {
                            return false;
                        }
                        return name.endsWith( suffix ); 
                    } )
                    .sorted( fileOrdering )
                    .map( resultCreator )
                    .collect( Collectors.toList() );
            return result;
        }
    }
}
