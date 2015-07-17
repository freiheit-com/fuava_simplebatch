package com.freiheit.fuava.simplebatch.result;


public class ResultStatistics {

	public static final class Builder<Input, Output> implements ProcessingResultListener<Input, Output> {

		private final Counts.Builder fetch = Counts.builder();
		private final Counts.Builder process = Counts.builder();
		private final Counts.Builder persist = Counts.builder();
		private boolean hasListenerDelegationFailures = false;
		
		public ResultStatistics build() {
			return new ResultStatistics(
					fetch.build(), 
					process.build(), 
					persist.build(),
					hasListenerDelegationFailures
				);
		}
		
		public void setListenerDelegationFailures(boolean b) {
			hasListenerDelegationFailures = b;
		}
		
		@Override
		public void onFetchResult(Result<?, Input> result) {
			fetch.count(result);
		}

		@Override
		public void onProcessingResult(Result<Input, Output> result) {
			process.count(result);		
		}

		@Override
		public void onPersistResult(Result<Input, ?> result) {
			persist.count(result);		
		}

	}
	
	private final Counts fetch;
	private final Counts process;
	private final Counts persist;
	private final boolean hasListenerDelegationFailures;
	
	public ResultStatistics(Counts fetch, Counts process, Counts persist, boolean hasListenerDelegationFailures) {
		this.fetch = fetch;
		this.process = process;
		this.persist = persist;
		this.hasListenerDelegationFailures = hasListenerDelegationFailures;
	}
	
	public Counts getFetchCounts() {
		return fetch;
	}
	
	public Counts getProcessingCounts() {
		return process;
	}
	
	public Counts getPersistCounts() {
		return persist;
	}
	
	
	private static boolean allFailed(Counts counts) {
		return counts.getError() != 0 && counts.getSuccess() == 0;
	}
	
	private static boolean allSuccess(Counts counts) {
		return counts.getError() == 0;
	}

	public boolean isAllFailed() {
		return allFailed(fetch) 
				||  allFailed(process) 
				|| allFailed(persist);

	}
	
	public boolean isAllSuccess() {		
		return allSuccess(fetch)
				&& allSuccess(process)
				&& allSuccess(persist)
				&& !hasListenerDelegationFailures();
	}
	
	public boolean hasListenerDelegationFailures() {
		return hasListenerDelegationFailures;
	}
	
	public static final <Input, Output> Builder<Input, Output> builder() {
		return new Builder<Input, Output>();
	}
}
