/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 14.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.simplebatch.fetch;

import java.io.File;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

/**
 * @author tim.lessner@freiheit.com
 */
class DirectoryFileFetcher<T> implements Supplier<Iterable<T>> {

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
    public Iterable<T> get() {
        final File dir = new File( uri );
        final File[] files = dir.listFiles( ( dir1, name ) -> {
            return name != null && name.endsWith( filter );
        } );

        if ( files == null ) {
            return ImmutableList.of();
        }
        final Iterable<File> sorted = FluentIterable.of( files ).toSortedList( fileOrdering );
        return FluentIterable.from( sorted ).transform( func ).toList();
    }
}
