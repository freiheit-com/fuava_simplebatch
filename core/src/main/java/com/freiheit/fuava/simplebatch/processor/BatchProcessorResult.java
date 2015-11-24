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