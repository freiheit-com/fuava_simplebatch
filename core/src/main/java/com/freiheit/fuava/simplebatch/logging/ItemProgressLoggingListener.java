/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 16.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.simplebatch.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public ItemProgressLoggingListener(String logName) {
        log = LoggerFactory.getLogger( logName );
    }

    @Override
    public void onBeforeRun(String description) {
        prefix = description;
    }

    @Override
    public void onFetchResult( final Result<Input, Input> result ) {
        if ( result.isFailed() ) {
            log.info( ResultItemStat.formatted(Event.FETCH, result.getFailureMessages(), result.getThrowables(),  result.getInput() ) );
        }
    }


    @Override
    public void onProcessingResult( final Result<Input, ?> result ) {
        if ( result.isFailed() ) {
            log.info( ResultItemStat.formatted( Event.PERSIST, result.getFailureMessages(), result.getThrowables(), result.getInput() ) );
        }
    }

}
