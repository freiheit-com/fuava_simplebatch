package com.freiheit.fuava.simplebatch.result;

public class ResultStatistics<Input, Output> {

	public static final class Counts {
		public static final class Builder {
			private int success;
			private int error;
			
			public Builder success() {
				success++;
				return this;
			}
			
			public Builder failed() {
				error++;
				return this;
			}
			
			public Counts build() {
				return new Counts(success, error);
			}
		}
		
		private final int success;
		private final int error;
		
		public Counts(int success, int error) {
			this.success = success;
			this.error = error;
		}
		
		
		public int getSuccess() {
			return success;
		}
		
		public int getError() {
			return error;
		}
		
		public static final Builder builder() {
			return new Builder();
		}
		
		
	}
	
	public static final class Builder<Input, Output> implements ProcessingResultListener<Input, Output> {

		private final Counts.Builder fetch = Counts.builder();
		private final Counts.Builder process = Counts.builder();
		private final Counts.Builder persist = Counts.builder();
		private boolean hasListenerDelegationFailures = false;
		
		public ResultStatistics<Input, Output> build() {
			return new ResultStatistics<Input, Output>(
					fetch.build(), 
					process.build(), 
					persist.build(),
					hasListenerDelegationFailures
				);
		}

		private void increment(Counts.Builder b, Result<?, ?> result) {
			if (result.isFailed()) {
				b.failed();
			} else {
				b.success();
			}
		}
		
		public void setListenerDelegationFailures(boolean b) {
			hasListenerDelegationFailures = b;
		}
		
		@Override
		public void onFetchResult(Result<?, Input> result) {
			increment(fetch, result);
		}

		@Override
		public void onProcessingResult(Result<Input, Output> result) {
			increment(process, result);		
		}

		@Override
		public void onPersistResult(Result<Input, ?> result) {
			increment(persist, result);		
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
