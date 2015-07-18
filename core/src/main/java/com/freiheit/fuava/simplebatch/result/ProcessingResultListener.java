package com.freiheit.fuava.simplebatch.result;

public interface ProcessingResultListener<Input, Output> {

    default void onBeforeRun(String description){
    }

    default void onAfterRun(){
    }

    default void onFetchResult(Result<Input, Input> result){
    }


    default void onFetchResults(Iterable<Result<Input, Input>> result) {
        for (Result<Input, Input> r: result) {
            onFetchResult(r);
        }
    }

    default void onProcessingResult(Result<Input, ?> result){

    }

    default void onProcessingResults(Iterable<? extends Result<Input, ?>> results) {
        for (Result<Input, ?> r: results) {
            onProcessingResult(r);
        }
    }

}
