package com.freiheit.fuava.simplebatch.processor;

public final class BatchProcessorResult<T> {
    private final int rowNum;
    private final T batchResult;
    private final int total;

    public BatchProcessorResult( final T batchResult, final int rowNum, final int total ) {
        this.batchResult = batchResult;
        this.rowNum = rowNum;
        this.total = total;

    }

    public int getRowNum() {
        return rowNum;
    }

    public int getTotal() {
        return total;
    }

    public T getBatchResult() {
        return batchResult;
    }
}