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

import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Function;

final class ContentProcessor<Input, Data>
        extends AbstractSingleItemProcessor<Input, Iterable<Result<FetchedItem<Data>, Data>>, ResultStatistics> {
    private final Function<Input, String> jobDescriptionFunc;
    private final int processingBatchSize;
    private final Processor<FetchedItem<Data>, Data, Data> contentProcessor;
    private final List<Function<? super Input, ProcessingResultListener<Data, Data>>> contentProcessingListeners;
    private final boolean parallelContent;
    private final Integer numParallelThreadsContent;
    private final int parallelTerminationTimeoutHours;

    public ContentProcessor(
            final Function<Input, String> jobDescriptionFunc,
            final int processingBatchSize,
            final boolean parallelContent,
            final Integer numParallelThreadsContent,
            final int parallelTerminationTimeoutHours,
            final Processor<FetchedItem<Data>, Data, Data> contentProcessor,
            final List<Function<? super Input, ProcessingResultListener<Data, Data>>> contentProcessingListeners ) {
        this.jobDescriptionFunc = jobDescriptionFunc;
        this.processingBatchSize = processingBatchSize;
        this.parallelContent = parallelContent;
        this.numParallelThreadsContent = numParallelThreadsContent;
        this.parallelTerminationTimeoutHours = parallelTerminationTimeoutHours;
        this.contentProcessor = contentProcessor;
        this.contentProcessingListeners = contentProcessingListeners;
    }

    @Override
    public Result<Input, ResultStatistics> processItem( final Result<Input, Iterable<Result<FetchedItem<Data>, Data>>> previous ) {
        if ( previous.isFailed() ) {
            // nothing we can do for failed processing items
            return Result.<Input, ResultStatistics> builder( previous ).failed();
        }
        final Input i = previous.getInput();
        final Iterable<Result<FetchedItem<Data>, Data>> output = previous.getOutput();
        final String desc = jobDescriptionFunc.apply( i );
        final BatchJob.Builder<Data, Data> builder =
                BatchJob.<Data, Data> builder()
                .setProcessingBatchSize( processingBatchSize )
                .setPrintFinalTimeMeasures( false )
                .setParallel( parallelContent )
                .setParallelTerminationTimeoutHours( parallelTerminationTimeoutHours )
                .setNumParallelThreads( numParallelThreadsContent )
                .setProcessor( contentProcessor );

        for ( final Function<? super Input, ProcessingResultListener<Data, Data>> listenerFactory : contentProcessingListeners ) {
            final ProcessingResultListener<Data, Data> listener = listenerFactory.apply( i );
            if ( listener != null ) {
                builder.addListener( listener );
            }
        }

        final BatchJob<Data, Data> job = builder.setFetcher( () -> output ).setDescription( desc ).build();
        final ResultStatistics statistics = job.run();

        if ( statistics.isAllFailed() ) {
            return Result.failed( i, "Processing of all Items failed. Please check the log files." );
        }

        return Result.success( i, statistics );
    }
   

}