package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.persist.FilePersistence;
import com.freiheit.fuava.simplebatch.persist.Persistence;
import com.freiheit.fuava.simplebatch.process.PrepareControlledFileProcessor;
import com.freiheit.fuava.simplebatch.process.Processor;
import com.freiheit.fuava.simplebatch.process.Processors;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

/**
 * An importer that imports files from the file system, adhering to the control file protocol.
 * @author klas
 *
 * @param <Output>
 */
public class FSImporterJob<Output>  extends BatchJob<ControlFile, Output> {
	
	public interface Configuration extends FilePersistence.Configuration {
		String getControlFileEnding();
		String getArchivedDirPath();
		String getProcessingDirPath();
	}

    public static final class ConfigurationImpl implements Configuration {

    	private String downloadDirPath;
    	private String archivedDirPath;
    	private String processingDirPath;
    	private String controlFileEnding;
    	
		@Override
		public String getDownloadDirPath() {
			return downloadDirPath;
		}
		
		public ConfigurationImpl setDownloadDirPath(String downloadDirPath) {
			this.downloadDirPath = downloadDirPath;
			return this;
		}

		@Override
		public String getArchivedDirPath() {
			return archivedDirPath;
		}
		
		public ConfigurationImpl setArchivedDirPath(String archivedDirPath) {
			this.archivedDirPath = archivedDirPath;
			return this;
		}

		@Override
		public String getProcessingDirPath() {
			return processingDirPath;
		}
		
		public ConfigurationImpl setProcessingDirPath(String processingDirPath) {
			this.processingDirPath = processingDirPath;
			return this;
		}

		public String getControlFileEnding() {
			return controlFileEnding;
		}
		
		public ConfigurationImpl setControlFileEnding(String controlFileEnding) {
			this.controlFileEnding = controlFileEnding;
			return this;
		}
    	
    }


	public static final class Builder<Output> {
		private final BatchJob.Builder<ControlFile, Output> builder = BatchJob.builder();
		private Configuration configuration;


		public Builder() {

		}

		public Builder<Output> setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}
		
		
		/**
		 * The number of files to read (and subsequently persist) together  in one batch.
		 */
		public Builder<Output> setContentBatchSize(
				int processingBatchSize) {
			builder.setProcessingBatchSize(processingBatchSize);
			return this;
		}


		public Builder<Output> setFileInputStreamReader(Function<InputStream, Iterable<Output>> documentReader) {
			builder.setReader(byIdsFetcher);
			return this;
		}

		public <P> Builder<Output> setContentPersistence(Function<List<Output>, List<P>> documentReader) {
			builder.setReader(byIdsFetcher);
			return this;
		}



		public Builder<Output> addListener(ProcessingResultListener<ControlFile, Output> listener) {
			builder.addListener(listener);
			return this;
		}


		public Builder<Output> removeListener(
				ProcessingResultListener<ControlFile, Output> listener) {
			builder.removeListener(listener);
			return this;
		}



		public FSImporterJob<Output> build() {
            new PrepareControlledFileProcessor<>( procDir, downloadDir ),
	        //always the same!
	        final Supplier<Iterable<ControlFile>> controlFileFilePagingFetcher = new DirectoryFileFetcher<ControlFile>(
	        		downloadDir, ".ctl", 
	        		new MakeControlFileFunction()
			);
    		//new ControlledFilePersistence()
    		/*
    		new Persistence<ControlFile, Iterable<ArticleCacheMiscData>>() {
        @Override
        public Iterable<? extends Result<ControlFile, ?>> persist(
                final Iterable<Result<ControlFile, Iterable<ArticleCacheMiscData>>> iterable ) {

            for ( final Result<ControlFile, Iterable<ArticleCacheMiscData>> r : iterable ) {
                final ControlFile input = r.getInput();
                final String pathname = procDir + "/" + input.getPathToControlledFile();

                System.out.println( r.isFailed() );
                System.out.println( r.getOutput() );
                for ( final ArticleCacheMiscData articleCacheMiscData : r.getOutput() ) {
                    try {
                        miscDocumentWriter.writeMiscDocumentToDb( articleCacheMiscData );
                        fileMover.moveFile( input.getFile(), archivedDir );
                        fileMover.moveFile( new File( pathname ), archivedDir );
                    } catch ( IOException | UpdateMiscDataFailedException e ) {
                        try {
                            fileMover.moveFile( input.getFile(), "/tmp/failed" );
                            fileMover.moveFile( new File( pathname ), "/tmp/failed" );
                        } catch ( FailedToMoveFileException e1 ) {
                            throw new RuntimeException();
                        }

                        e.printStackTrace();
                    } catch ( FailedToMoveFileException e ) {
                        e.printStackTrace();
                        throw new RuntimeException();
                    }
                }
            }
            return null;
        }
        */

			return new FSImporterJob<Output>(
					builder.getProcessingBatchSize(), 
					builder.getFetcher(), 
					builder.getProcessor(), 
					builder.getPersistence(),
					builder.getListeners()
			);
		}		
	}
	
	
	protected FSImporterJob(
			int processingBatchSize, 
			Fetcher<File> fetcher,
			Processor<File, Output> processor,
			Persistence<File, Output, ?> persistence,
			List<ProcessingResultListener<File, Output>> listeners
	) {
		super(processingBatchSize, fetcher, Processors.compose(processor, new PrepareControlledFile()), persistence, listeners);
	}

	
	
}
