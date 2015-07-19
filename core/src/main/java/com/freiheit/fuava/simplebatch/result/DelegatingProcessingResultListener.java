package com.freiheit.fuava.simplebatch.result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;

public class DelegatingProcessingResultListener<Input, Output> implements ProcessingResultListener<Input, Output> {
    static final Logger LOG = LoggerFactory.getLogger( DelegatingProcessingResultListener.class );

    private final Iterable<ProcessingResultListener<Input, Output>> listeners;

    private boolean hasDelegationFailures;

    public DelegatingProcessingResultListener(Iterable<ProcessingResultListener<Input, Output>> listeners) {
        this.listeners = listeners;
    }

    public boolean hasDelegationFailures() {
        return hasDelegationFailures;
    }

    protected void onListenerFailure(ProcessingResultListener<Input, Output> l, String fktName, Throwable t) {
        hasDelegationFailures = true;
        LOG.error("Failed to call Listener " + l + " for " + fktName, t);
    }

    @Override
    public void onBeforeRun(String description) {
        for (ProcessingResultListener<Input, Output> l :listeners) {
            try {
                l.onBeforeRun(description);
            } catch (Throwable t) {
                onListenerFailure(l, "onBeforeRun", t);
            }
        }
    }


    @Override
    public void onAfterRun() {
        for (ProcessingResultListener<Input, Output> l :listeners) {
            try {
                l.onAfterRun();
            } catch (Throwable t) {
                onListenerFailure(l, "onAfterRun", t);
            }
        }
    }

    @Override
    public void onFetchResult(Result<FetchedItem<Input>, Input> result) {
        for (ProcessingResultListener<Input, Output> l :listeners) {
            try {
                l.onFetchResult(result);
            } catch (Throwable t) {
                onListenerFailure(l, "onFetchResult", t);
            }
        }
    }

    @Override
    public void onFetchResults(Iterable<Result<FetchedItem<Input>, Input>> results) {
        for (ProcessingResultListener<Input, Output> l :listeners) {
            try {
                l.onFetchResults(results);
            } catch (Throwable t) {
                onListenerFailure(l, "onFetchResults", t);
            }
        }
    }


    @Override
    public void onProcessingResult(Result<FetchedItem<Input>, Output> result) {
        for (ProcessingResultListener<Input, Output> l :listeners) {
            try {
                l.onProcessingResult(result);
            } catch (Throwable t) {
                onListenerFailure(l, "onPersistResult", t);
            }
        }
    }

    @Override
    public void onProcessingResults(Iterable<? extends Result<FetchedItem<Input>, Output>> results) {
        for (ProcessingResultListener<Input, Output> l :listeners) {
            try {
                l.onProcessingResults(results);
            } catch (Throwable t) {
                onListenerFailure(l, "onPersistResults", t);
            }
        }
    }

}
