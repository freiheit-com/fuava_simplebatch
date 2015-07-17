package com.freiheit.fuava.simplebatch.result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public void onBeforeRun() {
		for (ProcessingResultListener<Input, Output> l :listeners) {
			try {
				l.onBeforeRun();
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
	public void onFetchResult(Result<?, Input> result) {
		for (ProcessingResultListener<Input, Output> l :listeners) {
			try {
				l.onFetchResult(result);
			} catch (Throwable t) {
				onListenerFailure(l, "onFetchResult", t);
			}
		}
	}

	@Override
	public void onFetchResults(Iterable<Result<?, Input>> results) {
		for (ProcessingResultListener<Input, Output> l :listeners) {
			try {
				l.onFetchResults(results);
			} catch (Throwable t) {
				onListenerFailure(l, "onFetchResults", t);
			}
		}
	}
	
	@Override
	public void onProcessingResult(Result<Input, Output> result) {
		for (ProcessingResultListener<Input, Output> l :listeners) {
			try {
				l.onProcessingResult(result);
			} catch (Throwable t) {
				onListenerFailure(l, "onProcessingResult", t);
			}
		}
	}

	@Override
	public void onProcessingResults(Iterable<Result<Input, Output>> results) {
		for (ProcessingResultListener<Input, Output> l :listeners) {
			try {
				l.onProcessingResults(results);
			} catch (Throwable t) {
				onListenerFailure(l, "onProcessingResults", t);
			}
		}
	}
	
	@Override
	public void onPersistResult(Result<Input, ?> result) {
		for (ProcessingResultListener<Input, Output> l :listeners) {
			try {
				l.onPersistResult(result);
			} catch (Throwable t) {
				onListenerFailure(l, "onPersistResult", t);
			}
		}
	}
	
	@Override
	public void onPersistResults(Iterable<? extends Result<Input, ?>> results) {
		for (ProcessingResultListener<Input, Output> l :listeners) {
			try {
				l.onPersistResults(results);
			} catch (Throwable t) {
				onListenerFailure(l, "onPersistResults", t);
			}
		}
	}
	
}
