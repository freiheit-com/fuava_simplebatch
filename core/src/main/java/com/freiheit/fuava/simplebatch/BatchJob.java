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
package com.freiheit.fuava.simplebatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import javax.annotation.CheckReturnValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.TimeLoggingProcessor;
import com.freiheit.fuava.simplebatch.result.DelegatingProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Downloads - or more generally, processes - data in two stages, via iterables.
 *
 * The output of the first stage is used as input for the second stage.
 *
 * A typical usecase would be to fetch Ids or URLs of the data to download in
 * the first stage ('source'), and then to fetch the data in the second stage
 * ('process'), persisting the data and the progress information in the third
 * stage 'sink'.
 *
 * The downloader uses iterables and does the stage 2 downloading in batches, so
 * you can provide iterables that stream over some data source, efficiently
 * processing huge amounts of data.
 *
 * @author Klas Kalass <klas.kalass@freiheit.com>
 *
 * @param <Input>
 *            The data fetched in stage one
 * @param <Output>
 *            The data fetched instage two
 */
public class BatchJob<Input, Output> {
    static final Logger LOG = LoggerFactory.getLogger( BatchJob.class );

    private final class CallProcessor implements Consumer<List<Result<FetchedItem<Input>, Input>>> {
        private final DelegatingProcessingResultListener<Input, Output> listeners;

        private CallProcessor( final DelegatingProcessingResultListener<Input, Output> listeners ) {
            this.listeners = listeners;
        }

        @Override
        public void accept( final List<Result<FetchedItem<Input>, Input>> sourceResults ) {

            listeners.onFetchResults( sourceResults );

            final Iterable<? extends Result<FetchedItem<Input>, Output>> processingResults = persistence.process( sourceResults );

            listeners.onProcessingResults( processingResults );
        }
    }

    public static class Builder<Input, Output> {
        private int processingBatchSize = 1000;
        private boolean parallel = false;
        private Fetcher<Input> fetcher;
        private Processor<FetchedItem<Input>, Input, Output> processor;

        private final ArrayList<ProcessingResultListener<Input, Output>> listeners =
                new ArrayList<ProcessingResultListener<Input, Output>>();
        private String description;

        public Builder() {
        }

        public int getProcessingBatchSize() {
            return processingBatchSize;
        }

        public boolean isParallel() {
            return parallel;
        }

        public Builder<Input, Output> setParallel( final boolean parallel ) {
            this.parallel = parallel;
            return this;
        }

        public Builder<Input, Output> setProcessingBatchSize( final int processingBatchSize ) {
            this.processingBatchSize = processingBatchSize;
            return this;
        }

        public Builder<Input, Output> setFetcher( final Fetcher<Input> idsFetcher ) {
            this.fetcher = idsFetcher;
            return this;
        }

        public Fetcher<Input> getFetcher() {
            return fetcher;
        }

        public Builder<Input, Output> setProcessor( final Processor<FetchedItem<Input>, Input, Output> writer ) {
            this.processor = writer;
            return this;
        }

        public Processor<FetchedItem<Input>, Input, Output> getProcessor() {
            return processor;
        }

        public Builder<Input, Output> addListener( final ProcessingResultListener<Input, Output> listener ) {
            this.listeners.add( listener );
            return this;
        }

        public Builder<Input, Output> addListeners( final Collection<ProcessingResultListener<Input, Output>> listeners ) {
            this.listeners.addAll( listeners );
            return this;
        }

        public Builder<Input, Output> removeListener( final ProcessingResultListener<Input, Output> listener ) {
            this.listeners.remove( listener );
            return this;
        }

        public Builder<Input, Output> removeListeners( final Collection<ProcessingResultListener<Input, Output>> listeners ) {
            this.listeners.removeAll( listeners );
            return this;
        }

        public ArrayList<ProcessingResultListener<Input, Output>> getListeners() {
            return listeners;
        }

        public Builder<Input, Output> setDescription( final String desc ) {
            this.description = desc;
            return this;
        }

        public BatchJob<Input, Output> build() {
            return new BatchJob<Input, Output>( description, processingBatchSize, parallel, fetcher, processor, listeners );
        }

        public String getDescription() {
            return description;
        }

    }

    private final int processingBatchSize;
    private final boolean parallel;
    private final Fetcher<Input> fetcher;
    private final Processor<FetchedItem<Input>, Input, Output> persistence;

    private final List<ProcessingResultListener<Input, Output>> listeners;
    private final String description;

    protected BatchJob(
            final String description,
            final int processingBatchSize,
            final boolean parallel,
            final Fetcher<Input> fetcher,
            final Processor<FetchedItem<Input>, Input, Output> persistence,
            final List<ProcessingResultListener<Input, Output>> listeners ) {
        this.description = description;
        this.processingBatchSize = processingBatchSize;
        this.parallel = parallel;
        this.fetcher = fetcher;
        this.persistence = persistence;
        this.listeners = ImmutableList.copyOf( listeners );
    }

    public static <Input, Output> Builder<Input, Output> builder() {
        return new Builder<Input, Output>();
    }

    @CheckReturnValue
    public final ResultStatistics run() {
        final ResultStatistics.Builder<Input, Output> resultBuilder = ResultStatistics.builder();

        final DelegatingProcessingResultListener<Input, Output> listeners =
                new DelegatingProcessingResultListener<Input, Output>(
                        ImmutableList.<ProcessingResultListener<Input, Output>> builder().add( resultBuilder ).addAll(
                                this.listeners ).build()
                );

        listeners.onBeforeRun( this.description );

        final Iterable<Result<FetchedItem<Input>, Input>> sourceIterable = fetcher.fetchAll();

        if ( sourceIterable instanceof Collection && this.persistence instanceof TimeLoggingProcessor ) {
            // Iterables could be lazy, but if it is a collection it should not be lazy so we can
            // count and report the result of the prepare stage.
            final Collection<?> collection = ( Collection<?> ) sourceIterable;
            ( ( TimeLoggingProcessor<?,?,?> ) this.persistence ).addNumPreparedItems( 
                    collection.size(), 
                    FluentIterable.from( collection ).filter( o -> ( ( Result<?, ?> ) o ).isSuccess() ).size(),
                    FluentIterable.from( collection ).filter( o -> ( ( Result<?, ?> ) o ).isFailed() ).size()
            );
        }
        final Iterable<List<Result<FetchedItem<Input>, Input>>> partitions = Iterables.partition( sourceIterable, processingBatchSize );
        
        StreamSupport.stream( partitions.spliterator(), parallel ).forEach( new CallProcessor( listeners ) );

        listeners.onAfterRun();
        resultBuilder.setListenerDelegationFailures( listeners.hasDelegationFailures() );

        if ( this.persistence instanceof TimeLoggingProcessor ) {
            ( ( TimeLoggingProcessor<?,?,?> ) this.persistence ).logFinalCounts();
        }
        // TODO: persist the state of the downloader (offset or downloader), so it can be
        //       provided the next time
        //idsDownloader.getWriteableState();
        return resultBuilder.build();
    }
}