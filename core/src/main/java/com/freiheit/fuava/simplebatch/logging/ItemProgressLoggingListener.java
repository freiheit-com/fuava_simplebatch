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
package com.freiheit.fuava.simplebatch.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;

/**
 * Logs the {@link ResultItemStat} for each downloaded, processed, or persisted
 * element if it has failed.
 *
 * @author tim.lessner@freiheit.com
 */
public class ItemProgressLoggingListener<Input, Output> implements ProcessingResultListener<Input, Output> {
    private final Logger log;
    private String prefix;

    public ItemProgressLoggingListener( final String logName ) {
        log = LoggerFactory.getLogger( logName );
    }

    @Override
    public void onBeforeRun( final String description ) {
        prefix = description;
    }

    @Override
    public void onFetchResult( final Result<FetchedItem<Input>, Input> result ) {
        if ( result.isFailed() ) {
            log.info( ResultItemStat.formatted( Event.FETCH, result.getFailureMessages(), result.getThrowables(),
                    result.getInput() ) );
        }
    }

    @Override
    public void onProcessingResult( final Result<FetchedItem<Input>, Output> result ) {
        if ( result.isFailed() ) {
            log.info( ResultItemStat.formatted( Event.PROCESSING, result.getFailureMessages(), result.getThrowables(),
                    result.getInput() ) );
        }
    }

}
