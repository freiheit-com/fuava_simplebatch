package com.freiheit.fuava.simplebatch.fetch;

import java.io.File;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

/**
 * @author tim.lessner@freiheit.com
 */
class DirectoryFileFetcher<T> implements Fetcher<T> {

    public static final Ordering<File> ORDERING_FILE_BY_PATH = Ordering.natural().onResultOf( File::getPath );
    private final String filter;
    private final String uri;
    private final Function<File, T> func;
    private final Ordering<File> fileOrdering;

    public DirectoryFileFetcher( final String uri, final String filter, final Function<File, T> func ) {
        this( uri, filter, func, ORDERING_FILE_BY_PATH );
    }

    public DirectoryFileFetcher( final String uri, final String filter, final Function<File, T> func,
            final Ordering<File> fileOrdering ) {
        this.fileOrdering = Preconditions.checkNotNull( fileOrdering );
        this.uri = Preconditions.checkNotNull( uri );
        this.filter = Preconditions.checkNotNull( filter );
        this.func = Preconditions.checkNotNull( func );
    }

    @Override
    public Iterable<Result<FetchedItem<T>, T>> fetchAll() {
        final File dir = new File( uri );
        final File[] files = dir.listFiles( ( dir1, name ) -> {
            return name != null && name.endsWith( filter );
        } );

        if ( files == null ) {
            return ImmutableList.of();
        }
        final Iterable<File> sorted = FluentIterable.of( files ).toSortedList( fileOrdering );

        final ImmutableList.Builder<Result<FetchedItem<T>, T>> b = ImmutableList.builder();
        int i = FetchedItem.FIRST_ROW;
        for ( final File f : sorted ) {
            final int rownum = i++;
            try {
                final T r = func.apply( f );
                final FetchedItem<T> fetchedItem = FetchedItem.<T> of( r, rownum );
                b.add( Result.success( fetchedItem, r ) );
            } catch ( final Throwable t ) {
                final FetchedItem<T> fetchedItem = FetchedItem.<T> of( null, rownum );
                b.add( Result.failed( fetchedItem, "Failed to read from " + f, t ) );
            }
        }
        return b.build();
    }
}
