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
package com.freiheit.fuava.simplebatch.logging;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.StreamSupport;

/**
 * Logs the {@link ResultBatchStat} of processing a single batch.
 *
 * @author tim.lessner@freiheit.com
 */
public class BatchStatisticsLoggingListener<Input, Output> implements ProcessingResultListener<Input, Output> {
    private final Logger log;
    private String _description;

    public BatchStatisticsLoggingListener( final String logFileName ) {
        log = LoggerFactory.getLogger( logFileName );
    }

    @Override
    public void onBeforeRun( final String description ) {
        _description = description;
    }

    @Override
    public void onFetchResults( final Iterable<Result<FetchedItem<Input>, Input>> result ) {
        final long failed = StreamSupport.stream( result.spliterator(), false ).filter( Result::isFailed ).count();
        final long success = StreamSupport.stream( result.spliterator(), false ).filter( Result::isSuccess ).count();
        log.info( ResultBatchStat.of( Event.FETCH, failed, success, _description ) );
    }

    @Override
    public void onProcessingResults( final Iterable<? extends Result<FetchedItem<Input>, Output>> iterable ) {
        final long failed = StreamSupport.stream( iterable.spliterator(), false ).filter( Result::isFailed ).count();
        final long success = StreamSupport.stream( iterable.spliterator(), false ).filter( Result::isSuccess ).count();
        log.info( ResultBatchStat.of( Event.PROCESSING, failed, success, _description ) );
    }
}
