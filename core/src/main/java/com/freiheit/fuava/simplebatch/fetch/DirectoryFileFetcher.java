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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
    private final String filter;
    private final Path uri;
    private final Function<Path, T> func;
    private final Ordering<Path> fileOrdering;

    public DirectoryFileFetcher( final Path uri, final String filter, final Function<Path, T> func ) {
        this( uri, filter, func, ORDERING_FILE_BY_PATH );
    }

    public DirectoryFileFetcher( final Path uri, final String filter, final Function<Path, T> func,
            final Ordering<Path> fileOrdering ) {
        this.fileOrdering = Preconditions.checkNotNull( fileOrdering );
        this.uri = Preconditions.checkNotNull( uri );
        this.filter = Preconditions.checkNotNull( filter );
        this.func = Preconditions.checkNotNull( func );
    }

    @Override
    public Iterable<Result<FetchedItem<T>, T>> fetchAll() {
        try {
        try (final Stream<Path> files = Files.walk( uri )) {
            final List<Result<FetchedItem<T>, T>> result = files
                .filter( path -> path.getFileName().toString().endsWith( filter ) )
                .sorted( fileOrdering )
                .map( new FetchFile<T>( this.func ) )
                .collect( Collectors.toList() );
            return result;
        }
        } catch ( final IOException e ) {
            return ImmutableList.of( Result.failed( FetchedItem.of( null, 0, uri.toString() ), "Could not traverse download directory") );
        }
        
    }
}
