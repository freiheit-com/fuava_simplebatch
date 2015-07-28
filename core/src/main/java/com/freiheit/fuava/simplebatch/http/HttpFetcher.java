package com.freiheit.fuava.simplebatch.http;

import java.io.InputStream;
import java.util.Map;

import com.freiheit.fuava.simplebatch.exceptions.FetchFailedException;
import com.google.common.base.Function;

public interface HttpFetcher {

    public <T> T fetch(
            final Function<? super InputStream, T> reader,
            final String uri,
            final Map<String, String> headers
            ) throws FetchFailedException;
}