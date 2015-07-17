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
import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;

/**
 * @author tim.lessner@freiheit.com
 */
class DirectoryFileFetcher<T> implements Supplier<Iterable<T>>{

    private final String filter;
	private final String uri;
	private Function<File, T> func;

    public DirectoryFileFetcher( final String uri, final String filter, Function<File, T> func) {
        this.uri = uri;
		this.filter = filter;
		this.func = func;
    }

    @Override
    public Iterable<T> get() {
        final File dir = new File( uri );
        final File[] files = dir.listFiles( ( dir1, name ) -> {
            return name.endsWith( filter );
        } );

        return FluentIterable.from(Arrays.asList(files)).transform(func);
    }}
