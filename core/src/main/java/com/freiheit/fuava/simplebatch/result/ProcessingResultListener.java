package com.freiheit.fuava.simplebatch.result;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;

public interface ProcessingResultListener<Input, Output> {

    default void onBeforeRun(String description){
    }

    default void onAfterRun(){
    }

    default void onFetchResult(Result<FetchedItem<Input>, Input> result){
    }


    default void onFetchResults(Iterable<Result<FetchedItem<Input>, Input>> result) {
        for (Result<FetchedItem<Input>, Input> r: result) {
            onFetchResult(r);
        }
    }

    default void onProcessingResult(Result<FetchedItem<Input>, Output> result){

    }

    default void onProcessingResults(Iterable<? extends Result<FetchedItem<Input>, Output>> results) {
        for (Result<FetchedItem<Input>, Output> r: results) {
            onProcessingResult(r);
        }
    }

}
