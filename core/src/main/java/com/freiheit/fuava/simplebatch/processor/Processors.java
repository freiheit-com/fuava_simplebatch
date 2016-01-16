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

import java.io.File;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.http.HttpDownloaderSettings;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.util.IOStreamUtils;
import com.google.common.base.Function;

public class Processors {

    public static final Logger LOG = LoggerFactory.getLogger( JsonLogger.class );

    /**
     * Takes the original input value and uses it as result, replacing the
     * previous result but keeping the success/failure state.
     * 
     * @author klas
     *
     * @param <Output>
     *            Type of the fetched Item
     * @param <Input>
     *            Type of the previous result
     */
    private static final class FetchedInputItemValueProcessor<Output, Input>
            extends AbstractSingleItemProcessor<FetchedItem<Output>, Input, Output> {
        @Override
        public Result<FetchedItem<Output>, Output> processItem(
                final Result<FetchedItem<Output>, Input> previous ) {
            final boolean failed = previous.isFailed();
            final FetchedItem<Output> item = previous.getInput();
            final Output input = item == null
                ? null
                : item.getValue();

            if ( failed ) {
                return Result.<FetchedItem<Output>, Output> builder( previous ).withOutput( input ).failed();
            } else {
                return Result.<FetchedItem<Output>, Output> builder( previous ).withOutput( input ).success();
            }
        }
    }

    /**
     * Takes the original input value and uses it as result, replacing the
     * previous result but keeping the success/failure state.
     * 
     * @author klas
     *
     * @param <Output>
     *            Type of the fetched Item
     * @param <Input>
     *            Type of the previous result
     */
    public static <Output, Input> Processor<FetchedItem<Output>, Input, Output> fetchedInputItemValueProcessor() {
        return new FetchedInputItemValueProcessor<Output, Input>();
    }
    
    /**
     * Takes the original input value and uses it as result, replacing the
     * previous result but keeping the success/failure state.
     * 
     * @author klas
     *
     * @param <Output>
     *            Type of the fetched Item
     * @param <Input>
     *            Type of the previous result
     */
    public static <Output, Input> Processor<FetchedItem<Output>, Input, Output> fetchedInputItemValueProcessor( final Class<Output> cls, final Class<Input> cls2 ) {
        return new FetchedInputItemValueProcessor<Output, Input>();
    }

    /**
     * Compose two processors. Note that the input of g will be a set of the
     * successful and failed output values from f. Also note that f must not
     * return null outputs for successfully processed items!
     */
    public static <OriginalItem, Input, Intermediate, Output> Processor<OriginalItem, Input, Output> compose( final Processor<OriginalItem, Intermediate, Output> g, final Processor<OriginalItem, Input, Intermediate> f ) {
        return new ComposedProcessor<OriginalItem, Input, Intermediate, Output>( g, f );
    }

    /**
     * Wraps a function that transforms a list of input values into a list of
     * output values and is expected to process or persist the data in some way.
     * Note that the function <b>must</b> support retries, meaning that a
     * failure in the processing of a list of items will lead to each item being
     * passed to the function in a singleton list.
     *
     * Thus you must make sure, that an item may be passed to your function two
     * times in a row, but only if the first call failed with an exception.
     *
     * Depending on your setup, you must ensure that your implementation opens a
     * transaction and performs a rollback if - and only if - an exception is
     * thrown.
     *
     */
    public static <OriginalItem, Input, Output> Processor<OriginalItem, Input, Output> retryableBatchedFunction(
            final Function<List<Input>, List<Output>> function ) {
        return new RetryingFunctionProcessor<OriginalItem, Input, Output>( function );
    }

    /**
     * Wraps a function that persists a single item.
     *
     *
     * Depending on your setup, you must ensure that your implementation opens a
     * transaction and performs a rollback if - and only if - an exception is
     * thrown.
     *
     * Please note that in most cases you should use
     * {@link #retryableBatchedFunction(Function)} and implement that one as
     * performant as possible.
     *
     * Persisting of each item independently usually is a lot slower, so use
     * this functionality only if it would not be faster to process the data in
     * a batch.
     *
     * @see #retryableBatchedFunction(Function)
     */
    public static <OriginalItem, Input, Output> Processor<OriginalItem, Input, Output> singleItemFunction(
            final Function<Input, Output> function ) {
        return new SingleItemFunctionProcessor<OriginalItem, Input, Output>( function );
    }

    /**
     * Persists the processed data to files in a directory. Each Item will be
     * written to a single file.
     *
     * @param baseDirPath
     *            the path tho the directory where each file is written to
     * @param adapter
     *            implements the logic for determining a file name and for
     *            writing data to a {@link Writer}.
     * @return a persistence which writes each pair of input/output items to a
     *         file in the given directory.
     */
    public static <OriginalItem, Input> Processor<OriginalItem, Input, FilePersistenceOutputInfo> fileWriter(
            final Path baseDirPath, final FileOutputStreamAdapter<OriginalItem, Input> adapter ) {
        return new FilePersistence<OriginalItem, Input>( baseDirPath, adapter );
    }

    /**
     * Like {@link #fileWriter(String, FileWriterAdapter)}, but additionally
     * writes a control-file that can be used for waiting until this file has
     * been completely written as well as a log file with extra information
     */
    public static <OriginalInput, Input> Processor<FetchedItem<OriginalInput>, Input, ControlFilePersistenceOutputInfo> controlledFileWriter(
            final Path baseDirPath,
            final String controlFileEnding,
            final String logFileEnding,
            final FileOutputStreamAdapter<FetchedItem<OriginalInput>, Input> adapter ) {
        return
            // Write File
            new FilePersistence<FetchedItem<OriginalInput>, Input>( baseDirPath, adapter )
            // Write Log
            .then(new JsonLoggingProcessor<OriginalInput>( baseDirPath, controlFileEnding, logFileEnding ) )
            // Move File, Log File and Control File 
            .then(new ControlFilePersistence<FetchedItem<OriginalInput>>(
                new ControlFilePersistenceConfigImpl( baseDirPath, controlFileEnding, logFileEnding ) ));
    }

    /**
     * Writes a batch (aka partition) of the processed data into one file.
     *
     * Each processed item will get an output info instance which provides
     * details about the file and the item number within that file (for csv this
     * will probably correspond to the rownum, depending on your implementation
     * of the adapter)
     *
     * @return
     */
    public static <OriginalItem, Input> Processor<OriginalItem, Input, BatchProcessorResult<FilePersistenceOutputInfo>> batchFileWriter(
            final Path baseDirPath, final FileWriterAdapter<List<OriginalItem>, List<Input>> adapter ) {
        return new BatchedSuccessesProcessor<OriginalItem, Input, FilePersistenceOutputInfo>( fileWriter( baseDirPath, adapter ) );
    }

    /**
     * Writes a batch (aka partition) of the processed data into one
     * (controlled) file.
     *
     * Each processed item will get an output info instance which provides
     * details about the file and the item number within that file (for csv this
     * will probably correspond to the rownum, depending on your implementation
     * of the adapter)
     *
     * This is very similar to
     * {@link #batchFileWriter(String, FileWriterAdapter)}, but for each
     * persisted file there will be a control file and a log file as well.
     */
    public static <OriginalInput, Input> Processor<FetchedItem<OriginalInput>, Input, BatchProcessorResult<ControlFilePersistenceOutputInfo>> controlledBatchFileWriter(
            final Path baseDirPath,
            final String controlFileEnding,
            final String logFileEnding,
            final FileOutputStreamAdapter<List<FetchedItem<OriginalInput>>, List<Input>> adapter ) {
        return 
            new JsonLoggingBatchedFailureProcessor<OriginalInput, Input>( baseDirPath, controlFileEnding, logFileEnding ) 
            .then(new BatchedSuccessesProcessor<FetchedItem<OriginalInput>, Input, ControlFilePersistenceOutputInfo>(
                    // "Main" processing pipeline, where not a list of results is processed, but a result with a list of successfull items
                    // write the batch file with the successfull items
                    new FilePersistence<List<FetchedItem<OriginalInput>>, List<Input>>( baseDirPath, adapter )
                    // add the log entries for the batched items
                    .then( new JsonLoggingBatchedSuccessProcessor<OriginalInput>( logFileEnding ) )
                    // move the batch file and the control file
                    .then( new ControlFilePersistence<List<FetchedItem<OriginalInput>>>( new ControlFilePersistenceConfigImpl( baseDirPath, controlFileEnding, logFileEnding ) ))
                )
            );
    }

    /**
     * A persistence that takes the Iterable which is passed as a data item and
     * uses it as input to the job builder, which it uses to create a job that
     * will subsequently be executed as an inner job.
     *
     * Note that this Persistence always works on a single item of the input
     * data, which must be an iterable.
     *
     * Note that this means, that the instances provided to the builder will be
     * used for multiple instances of the (inner) BatchJob.
     */
    public static <OriginalItem, InnerInput> Processor<OriginalItem, Iterable<Result<FetchedItem<InnerInput>, InnerInput>>, ResultStatistics> runBatchJobProcessor(
            final Function<OriginalItem, String> jobDescriptionFunc, final int processingBatchSize,
            final Processor<FetchedItem<InnerInput>, InnerInput, InnerInput> contentProcessor,
            final List<Function<? super OriginalItem, ProcessingResultListener<InnerInput, InnerInput>>> contentProcessingListeners ) {
        return runBatchJobProcessor( jobDescriptionFunc, processingBatchSize,
                false /* not parallel */, contentProcessor, contentProcessingListeners );
    }


    /**
     * A persistence that takes the Iterable which is passed as a data item and
     * uses it as input to the job builder, which it uses to create a job that
     * will subsequently be executed as an inner job.
     *
     * Note that this Persistence always works on a single item of the input
     * data, which must be an iterable.
     *
     * Note that this means, that the instances provided to the builder will be
     * used for multiple instances of the (inner) BatchJob.
     */
    public static <OriginalItem, InnerInput> Processor<OriginalItem, Iterable<Result<FetchedItem<InnerInput>, InnerInput>>, ResultStatistics> runBatchJobProcessor(
            final Function<OriginalItem, String> jobDescriptionFunc, final int processingBatchSize,
            final boolean parallelContent,
            final Processor<FetchedItem<InnerInput>, InnerInput, InnerInput> contentProcessor,
            final List<Function<? super OriginalItem, ProcessingResultListener<InnerInput, InnerInput>>> contentProcessingListeners ) {
        return new ContentProcessor<OriginalItem, InnerInput>( jobDescriptionFunc, processingBatchSize, parallelContent,
                contentProcessor,
                contentProcessingListeners );
    }

    /**
     * A Processor that uses an apache HttpClient to download the required data,
     * based on the input data that was provided by the fetcher.
     */
    public static <OriginalItem, Input, Output> Processor<OriginalItem, Input, Output> httpDownloader(
            final HttpClient client,
            final HttpDownloaderSettings<Input> settings,
            final Function<InputStream, Output> converter ) {
        return new HttpDownloader<OriginalItem, Input, Output>( client, settings, converter );
    }

    /**
     * A Processor that uses an apache HttpClient to download the required data
     * as string, based on the input data that was provided by the fetcher.
     */
    public static <OriginalItem, Input> Processor<OriginalItem, Input, String> httpDownloader(
            final HttpClient client,
            final HttpDownloaderSettings<Input> settings ) {
        return new HttpDownloader<OriginalItem, Input, String>( client, settings, input -> IOStreamUtils.consumeAsString( input ) );
    }

    /**
     * A Processor which reads control files and moves them (and the file
     * referenced by the control file) to the targetDir. 
     *
     *
     */
    public static <Data> Processor<FetchedItem<ControlFile>, Data, Data> toArchiveDirMover( final Path targetDir, final Path failedDir ) {
        return new ToArchiveDirMover<Data>( targetDir, failedDir );
    }

    /**
     * A Processor which reads control files and moves them (and the file
     * referenced by the control file) to the targetDir. Returns the moved file.
     */
    public static Processor<FetchedItem<ControlFile>, ControlFile, File> toProcessingDirMover( final Path processingDir, final String instanceId ) {
        return new ToProcessingDirMover( processingDir, instanceId );
    }

    /**
     * A Processor which moves files.
     * 
     * @return
     */
    public static <OriginalItem> Processor<OriginalItem, File, File> fileMover( final String targetDir ) {
        return new FileMovingProcessor<OriginalItem>( targetDir );
    }

}
