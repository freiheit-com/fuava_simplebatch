package com.freiheit.fuava.simplebatch.persist;

import java.io.Writer;
import java.util.List;

import com.google.common.base.Function;

public class Persistences {

     /**
     * Compose two processors. Note that the input of g will be a set of the
     * successful and failed output values from f. Also note that f must not
     * return null outputs for successfully processed items!
     */
    public static <A, B, C, D> Persistence<A, B, D> compose( Persistence<A, C, D> g, Persistence<A, B, C> f ) {
        return new ComposedPersistence<A, B, C, D>( g, f );
    }

    /**
     * Wraps a function that transforms a list into a list and is expected to persist the data in some way.
     * Note that the function <b>must</b> support retries, meaning that a failure in the processing of a list of items
     * will lead to each item being passed to the function in a singleton list. 
     * 
     * Thus you must make sure, that an item may be passed to your function two times in a row, but only if the first
     * call failed with an exception.
     * 
     * Depending on your setup, you must ensure that your implementation opens a transaction and performs a rollback 
     * if - and only if - an exception is thrown.
     * 
     */
    public static <JobInput, PersistenceInput, PersistenceOutput> Persistence<JobInput, PersistenceInput, PersistenceOutput> retryableBatch( 
    		Function<List<PersistenceInput>, List<PersistenceOutput>> function
	) {
        return new RetryingPersistence<JobInput, PersistenceInput, PersistenceOutput>(function);
    }
    
    /**
     * Wraps a function that persists a single item.
     * 
     * 
     * Depending on your setup, you must ensure that your implementation opens a transaction and performs a rollback 
     * if - and only if - an exception is thrown.
     * 
     * Please note that in most cases you should use {@link #retryableBatch(Function)} and implement that one as performant as possible.
     * 
     * Persisting of each item independently usually is a lot slower, so use this functionality only if it would not be faster to process the data in a batch.
     * 
     * @see #retryableBatch(Function)
     */
    public static <JobInput, PersistenceInput, PersistenceOutput> Persistence<JobInput, PersistenceInput, PersistenceOutput> single( 
    		Function<PersistenceInput, PersistenceOutput> function
	) {
        return new SingleItemPersistence<JobInput, PersistenceInput, PersistenceOutput>(function);
    }
    
    /**
     * Persists the processed data to files in a directory. Each Item will be written to a single file.
     * 
     * @param dirName the path tho the directory where each file is written to
     * @param adapter implements the logic for determining a file name and for writing data to a {@link Writer}.
     * @return a persistence which writes each pair of input/output items to a file in the given directory.
     */
    public static <Input, Output> Persistence<Input, Output, FilePersistenceOutputInfo> file(
    		String dirName, PersistenceAdapter<Input, Output> adapter
	) {
    	return new FilePersistence<Input, Output>(dirName, adapter);
    }
    
    /**
     * Like {@link #file(String, PersistenceAdapter)}, but additionally writes a control-file that can be used for
     * waiting until this file has been completely written.
     * 
     * @param dirName
     * @param controlFileEnding
     * @param adapter
     * @return
     */
    public static <Input, Output> Persistence<Input, Output, ControlFilePersistenceOutputInfo> controlledFile(
    		final String dirName,
    		final String controlFileEnding,
    		PersistenceAdapter<Input, Output> adapter
	) {
    		return Persistences.compose(
                    new ControlFilePersistence<Input>( new ControlFilePersistenceConfigImpl(dirName, controlFileEnding) ),
                    new FilePersistence<Input, Output>( dirName, adapter )
            );

    }
     
    /**
     * Writes a batch (aka partition) of the processed data into one file. 
     * 
     * Each processed item will get an output info instance which provides details about the file 
     * and the item number within that file (for csv this will probably correspond to the rownum, depending on your implementation of the adapter)
     * @return
     */
    public static <Input, Output> Persistence<Input, Output, BatchPersistenceResult<FilePersistenceOutputInfo>> batchFile(
    		String dirName, PersistenceAdapter<List<Input>, List<Output>> adapter
	) {
    	return new BatchPersistence<Input, Output, FilePersistenceOutputInfo>(file(dirName, adapter));
    }
    
    /**
     * Writes a batch (aka partition) of the processed data into one (controlled) file. 
     * 
     * Each processed item will get an output info instance which provides details about the file 
     * and the item number within that file (for csv this will probably correspond to the rownum, depending on your implementation of the adapter)
     * 
     * This is very similar to {@link #batchFile(String, PersistenceAdapter)}, but for each persisted file there will be a control file as well.
     * 
     * @return
     */
    public static <Input, Output> Persistence<Input, Output, BatchPersistenceResult<ControlFilePersistenceOutputInfo>> controlledBatchFile(
    		String dirName, 
    		String controlFileEnding,
    		PersistenceAdapter<List<Input>, List<Output>> adapter
	) {
    	return new BatchPersistence<Input, Output, ControlFilePersistenceOutputInfo>(controlledFile(dirName, controlFileEnding, adapter));
    }
    

}
