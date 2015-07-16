package com.freiheit.fuava.simplebatch.result;

public interface ProcessingResultListener<Input, Output> {
	default void onFetchResult(Result<?, Input> result){
	}
	
	default void onFetchResults(Iterable<Result<?, Input>> result) {
		for (Result<?, Input> r: result) {
			onFetchResult(r);
		}
	}
	
	default void onProcessingResult(Result<Input, Output> result){

	}
	
	default void onProcessingResults(Iterable<Result<Input, Output>> results) {
		for (Result<Input, Output> r: results) {
			onProcessingResult(r);
		}
	}

	default void onPersistResult(Result<Input, ?> result){

	}
	
	default void onPersistResults(Iterable<? extends Result<Input, ?>> results) {
		for (Result<Input, ?> r: results) {
			onPersistResult(r);
		}
	}

}
