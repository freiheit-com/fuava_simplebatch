package com.freiheit.fuava.simplebatch.processor;

import java.io.File;
import java.io.InputStream;
import java.io.Writer;
import java.util.List;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.http.HttpDownloaderSettings;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Function;

public class Processors {

    /**
     * Compose two processors. Note that the input of g will be a set of the
     * successful and failed output values from f. Also note that f must not
     * return null outputs for successfully processed items!
     */
    public static <A, B, C, D> Processor<A, B, D> compose( final Processor<A, C, D> g, final Processor<A, B, C> f ) {
        return new ComposedProcessor<A, B, C, D>( g, f );
    }

    /**
     * Combine two processors. 
     * Runs both processor on the same input and collects outputs.
     */
    public static <A, B, C> Processor<A, B, C> parallel( final Processor<A, B, C> g, final Processor<A, B, C> f ) {
        return new ParallelProcessor<A, B, C>( g, f );
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
    public static <JobInput, PersistenceInput, PersistenceOutput> Processor<JobInput, PersistenceInput, PersistenceOutput> retryableBatchedFunction(
            final Function<List<PersistenceInput>, List<PersistenceOutput>> function
            ) {
        return new RetryingProcessor<JobInput, PersistenceInput, PersistenceOutput>( function );
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
    public static <JobInput, PersistenceInput, PersistenceOutput> Processor<JobInput, PersistenceInput, PersistenceOutput> singleItemFunction(
            final Function<PersistenceInput, PersistenceOutput> function
            ) {
        return new SingleItemProcessor<JobInput, PersistenceInput, PersistenceOutput>( function );
    }

    /**
     * Persists the processed data to files in a directory. Each Item will be
     * written to a single file.
     *
     * @param dirName
     *            the path tho the directory where each file is written to
     * @param adapter
     *            implements the logic for determining a file name and for
     *            writing data to a {@link Writer}.
     * @return a persistence which writes each pair of input/output items to a
     *         file in the given directory.
     */
    public static <Input, Output> Processor<Input, Output, FilePersistenceOutputInfo> fileWriter(
            final String dirName, final FileOutputStreamAdapter<Input, Output> adapter
            ) {
        return new FilePersistence<Input, Output>( dirName, adapter );
    }

    /**
     * Like {@link #fileWriter(String, FileWriterAdapter)}, but additionally
     * writes a control-file that can be used for waiting until this file has
     * been completely written.
     *
     * @param dirName
     * @param controlFileEnding
     * @param adapter
     * @return
     */
    public static <Input, Output> Processor<Input, Output, ControlFilePersistenceOutputInfo> controlledFileWriter(
            final String dirName,
            final String controlFileEnding,
            final FileOutputStreamAdapter<Input, Output> adapter
            ) {
        return Processors.compose(
                new ControlFilePersistence<Input>( new ControlFilePersistenceConfigImpl( dirName, controlFileEnding ) ),
                new FilePersistence<Input, Output>( dirName, adapter )
                );

    }

    
    /**
     * Like {@link #fileWriter(String, FileWriterAdapter)}, but additionally
     * writes a control-file that can be used for waiting until this file has
     * been completely written. Unlike {@link #controlledFileWriter(String, String, FileOutputStreamAdapter<Input, Output>)}
     * makes two copies of the file, each in it's own folder.
     *
     * @param dirName
     * @param controlFileEnding
     * @param adapter
     * @return
     */
    public static <Input, Output> Processor<Input, Output, ControlFilePersistenceOutputInfo> controlledTwinFileWriter(
            final String dirName,
            final String controlFileEnding,
            final FileOutputStreamAdapter<Input, Output> firstAdapter,
            final String firstAdapterRelativePath,
            final FileOutputStreamAdapter<Input, Output> secondAdapter,
            final String secondAdapterRelativePath
            ) {

    	Processor<Input, Output, ControlFilePersistenceOutputInfo> firstProcessor = Processors.compose(
                new ControlFilePersistence<Input>( new ControlFilePersistenceConfigImpl( dirName + firstAdapterRelativePath, controlFileEnding ) ),
    			new FilePersistence<Input, Output>( dirName + firstAdapterRelativePath, firstAdapter )
    	);

    	Processor<Input, Output, ControlFilePersistenceOutputInfo> secondProcessor = Processors.compose(
                new ControlFilePersistence<Input>( new ControlFilePersistenceConfigImpl( dirName + secondAdapterRelativePath, controlFileEnding ) ),
                new FilePersistence<Input, Output>( dirName + secondAdapterRelativePath, secondAdapter )
                );

    	return Processors.parallel(secondProcessor, firstProcessor);
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
    public static <Input, Output> Processor<Input, Output, BatchProcessorResult<FilePersistenceOutputInfo>> batchFileWriter(
            final String dirName, final FileWriterAdapter<List<Input>, List<Output>> adapter
            ) {
        return new BatchProcessor<Input, Output, FilePersistenceOutputInfo>( fileWriter( dirName, adapter ) );
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
     * persisted file there will be a control file as well.
     *
     * @return
     */
    public static <Input, Output> Processor<Input, Output, BatchProcessorResult<ControlFilePersistenceOutputInfo>> controlledBatchFileWriter(
            final String dirName,
            final String controlFileEnding,
            final FileOutputStreamAdapter<List<Input>, List<Output>> adapter
            ) {
        return new BatchProcessor<Input, Output, ControlFilePersistenceOutputInfo>( controlledFileWriter( dirName,
                controlFileEnding, adapter ) );
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
    public static <Input, Data> Processor<Input, Iterable<Data>, ResultStatistics> runBatchJobProcessor(
            final Function<Input, String> jobDescriptionFunc,
            final BatchJob.Builder<Data, Data> builder
            ) {
        return new InnerJobProcessor<Input, Data>( jobDescriptionFunc, builder );
    }

    /**
     * A Processor that uses an apache HttpClient to download the required data,
     * based on the input data that was provided by the fetcher.
     */
    public static <I, Input, Output> Processor<I, Input, Output> httpDownloader(
            final HttpClient client,
            final HttpDownloaderSettings<Input> settings,
            final Function<InputStream, Output> converter
            ) {
        return new HttpDownloader<I, Input, Output>( client, settings, converter );
    }

    /**
     * A Processor which reads control files and moves them (and the file
     * referenced by the control file) to the targetDir. Returns the moved file.
     *
     *
     */
    public static <Input> Processor<Input, ControlFile, File> controlledFileMover( final String targetDir ) {
        return new ControlledFileMovingProcessor<Input>( targetDir );
    }

    /**
     * A Processor which moves files.
     * 
     * @return
     */
    public static <Input> Processor<Input, File, File> fileMover( final String targetDir ) {
        return new FileMovingProcessor<Input>( targetDir );
    }
}
