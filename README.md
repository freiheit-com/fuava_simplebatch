# Fuava SimpleBatch
A Java library that makes implementation of simple, fast and failsafe batch processing jobs nearly trivial.

It provides many helpers and default implementations for everyday batch processing tasks.

With SimpleBatch, you can write your Jobs such that they comply to the following standards
  - **Isolate Failures**. If Processing or Persisting of one Item fails, everything else continues.
  - **Process Batches**. A lot of tasks (for example Database queries) are a lot faster if performed for multiple items at once.
  - **Iterate over large Datasets**. If your Input Iterable is lazy, you can process huge datasets where only the currently processed batches are kept in memory at any one time.
  - **Communicate processing status**. You will recieve processing statistics at the end of your job and you can register listeners to keep you informed.

We provide classes to streamline the following tasks
  - Any type of plain old BatchJob that has a **data fetching, a data processing and a persistence stage**.
  - **Downloader / Importer pair** of Command Line Tools (or jobs, if you prefer) that communicate via the filesystem. The Downloader will create control files which will then be used by the importer to read the downloaded file, move it to a processing folder, process it and in the end move it to an archive folder, or failed folder if it failed completely.
  - **Simple Command line tool** that perform some batch job, print statistics and fail if and only if all items failed


## Downloader (works with Control-File)



## Importer (also works with Control-File)

Example for an importer (works together with the above downloader) from a json file with lists of Article.
```java
final CtlImporterJob<Article> fileProcessingJob = new CtlImporterJob.Builder<Article>()

        // provide settings: input directory, archive directory, processing directory etc.
	.setConfiguration( config )

        // The function to read the input stream of the files
	.setFileInputStreamReader((InputStream is) -> new Gson().fromJson(new InputStreamReader(is), Types.listOf(Integer.class)))

        // the number of Article items to persist together in one list
	.setContentBatchSize( 100 )

        // Persist the lists (max. size is set to 100 above) 
	.setContentPersistence(

                // the function will be called again with singleton lists of the given items, if processing
                // of the original list fails
	        Persistences.retryableBatchedFunction(new Function<List<Article>, List<Article>>() {
	        	@Override
	        	public List<Integer> apply(List<Article> data) {
	        		// store data in database
	        		db.storeAll(data);
	        		return data;
	        	}
	        })
	)
	.build();

// Run the job. This reads the control files from download directory, 
// moves the control  file (and its data file) to the processing dir.
// Then it reads the data files, persists the content with the given function and
// finally moves the file (together with its control file) to the archive 
// directory, or the failed directory if all items in the file failed
fileProcessingJob.run();

```

If you implement a command line tool, you should call `CtlImporterJobMain.exec( fileProcessingJob )` 
instead of `fileProcessingJob.run()`. This will lead to statistics on the command line, and `System.exit(-1)` for completely failing jobs.

[BatchJob.java](https://github.com/freiheit-com/fuava_simplebatch/blob/master/core/src/main/java/com/freiheit/fuava/simplebatch/BatchJob.java)

