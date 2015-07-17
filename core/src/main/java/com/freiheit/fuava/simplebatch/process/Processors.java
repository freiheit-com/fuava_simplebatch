package com.freiheit.fuava.simplebatch.process;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.http.HttpDownloaderSettings;
import com.google.common.base.Function;


public class Processors {
	/**
	 * Compose two processors.
	 * Note that the input of g will be a set of the successful output values from f.
	 * Also note that f must not return null outputs for successfully processed items!
	 */
	public static <A, B, C> Processor<A, C> compose(Processor<B, C> g, Processor<A, B> f) {
		return new ComposedProcessor<A, B, C>(g, f);
	}

	/**
	 * A Processor that uses an apache HttpClient to download the required data, based on the input data 
	 * that was provided by the fetcher.
	 */
	public static <Input, Output> Processor<Input, Output> httpDownloader(
			final HttpClient client, 
    		final HttpDownloaderSettings<Input> settings,
    		final Function<InputStream, Output> converter
	) {
		return new HttpDownloader<Input, Output>(client, settings, converter);
	}

	/**
	 * A processor that takes the Iterable which is passed as a data item to the returned processor and 
	 * uses it as input to the job builder, which it uses to create a job that will subsequently be executed as an inner job.
	 * 
	 * Note that this Processor always works on a single item of the input data, which must be an iterable.
	 * 
	 * Note that this means, that the instances provided to the builder will be used for multiple instances of the (inner) BatchJob.
	 */
	public static <Data> Processor<Iterable<Data>, Iterable<Data>> runSingleItemBatchJobProcessor(BatchJob.Builder<Data, Data> builder) {
		return new InnerJobProcessor<Data>(builder);
	}
	
	/**
	 * A Processor that does nothing.
	 */
	public static <A> Processor<A, A> identity() {
		return new IdentityProcessor<A>();
	}

	/**
	 * A Processor that processes a single item with the help of a function. 
	 * If it would be faster to perform the processing in batches (i. e. if you use databases), 
	 * use {@link #retryableBatch(Function)}.
	 * 
	 * @see #retryableBatch(Function)
	 */
	public static <Input, Output> Processor<Input, Output> single(Function<Input, Output> reader) {
		return new SingleItemProcessor<Input, Output>(reader);
	}

	/**
	 * A processor which delegates processing of batches to a function.
	 * 
	 * If processing of a batch failed, it will be divided into singleton batches and retried.
	 * 
	 * You have to ensure, that aborting and retying the function will not lead to illegal states.
	 * 
	 * If your function persists to databases for example, you may need to ensure that your function open
	 * and closes the toplevel transaction and rolls back for <b>all</b> exceptions.
	 * @param function
	 * @return
	 */
	public static <Input, Output> Processor<Input, Output> retryableBatch( 
			Function<List<Input>, List<Output>> function
	) {
		return new RetryingProcessor<Input, Output>(new MapBuildingFunction<Input, Output>(function));
	}

	/**
	 * Like {@link #retryableBatch(Function)}, but takes a function which produces a map from input to output.
	 */
	public static <Input, Output> Processor<Input, Output> retryableToMapBatch( 
			Function<List<Input>, Map<Input, Output>> function
	) {
		return new RetryingProcessor<Input, Output>(function);
	}

	
	/**
	 * A Processor which reads control files and moves them (and the file referenced by the control file) to the targetDir.
	 * Returns the moved file.
	 * 
	 * 
	 */
	public static Processor<ControlFile, File> controlledFileMover(String targetDir) {
		return new ControlledFileMovingProcessor(targetDir);
	}
	
	/**
	 * A Processor which moves files.
	 * @return
	 */
	public static Processor<File, File> fileMover(String targetDir) {
		return new FileMovingProcessor(targetDir);
	}
}
