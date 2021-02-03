# Fuava SimpleBatch
A Java library that aims to make implementation of simple, fast and failsafe batch processing jobs nearly trivial.

It provides many helpers and default implementations for everyday batch processing tasks.

With SimpleBatch, you can write your Jobs such that they comply to the following standards
  - **Isolate Failures**. If Processing or Persisting of one Item fails, everything else continues.
  - **Process Batches**. A lot of tasks (for example Database queries) are a lot faster if performed for multiple items at once.
  - **Iterate over large Datasets**. If your Input Iterable is lazy, you can process huge datasets where only the currently processed batches are kept in memory at any one time.
  - **Communicate processing status**. You will receive processing statistics at the end of your job and you can register listeners to keep you informed.

## Basic Usage

[Working Example in Tests](https://github.com/freiheit-com/fuava_simplebatch/blob/master/core/src/test/java/com/freiheit/fuava/simplebatch/example/SimpleLoopTest.java)
```java
// Will be used to collect the number of successfully processed (or failed)  items 
final Counts.Builder statistics = Counts.builder();

// A fetcher will retrieve data from somewhere - here it simply works on a list
final Fetcher<Integer> fetcher = Fetchers.iterable(ImmutableList.<Integer>of(1, 2, 3, 4));

// A processor takes a list of inputs and does interesting things to them, 
// for example downloading data or persisting data.
final Processor<FetchedItem<Integer>, Integer, Article> processor = 
    // Retryable means, that the function will be called again if a list with more
    // than one item fails during processing. It will be called for each of those items 
    // seperately, wrapped in a singleton list
	Processors.retryableBatchedFunction(new Function<List<Integer>, List<Article>>() {

		@Override
		public List<Article> apply(List<Integer> ids) {
			// Do interesting stuff, maybe using the ids to fetch the Article
			// and then to store it
			return ids.stream().map(id -> new Article(id)).collect(Collectors.toList());
		}
	});

// Build partitions to iterate over. This means, that your processor will
// always work on maximum 100 items
Iterable<List<Result<FetchedItem<Integer>, Integer>>> partitions = Iterables.partition( 
	fetcher.fetchAll(), 100 
);

// Do the real work: iterate over the input data and pass it to the processing stage
for ( List<Result<FetchedItem<Integer>, Integer>> sourceResults : partitions) {
	statistics.addAll(processor.process( sourceResults ));
}

// Do something useful with the information collected. If you only got errors
// you might want to throw an exception 
Counts counts = statistics.build();
System.out.println("Num Errors: " + counts.getError());
System.out.println("Num Success: " + counts.getSuccess());

```

## Job-Builders available:

We provide classes to streamline the following tasks
  - Any type of plain old BatchJob that has a **data fetching and a processing or persistence stage**.
  - **Downloader / Importer pair** of Command Line Tools (or jobs, if you prefer) that communicate via the filesystem. The Downloader will create control files which will then be used by the importer to read the downloaded file, move it to a processing folder, process it and in the end move it to an archive folder, or failed folder if it failed completely.
  - **Simple Command line tool** that perform some batch job, print statistics and fail if and only if all items failed

### General BatchJob
The general pattern for implementing a batch job (no matter wether files are used or not) is:


```java
final BatchJob<Input, ProcessedData> job = 
    new BatchJob.Builder<Input, ProcessedData>()
        // fetches the data from any source, the returned iterable may be lazy
        .setFetcher( Fetchers....)
        // processes (often persists) partitions of the iterable provided by the fetcher
        .setProcessor( Processors....)
        .setProcessingBatchSize( 100 )
        .build();
job.run();
```


[BatchJob.java](https://github.com/freiheit-com/fuava_simplebatch/blob/master/core/src/main/java/com/freiheit/fuava/simplebatch/BatchJob.java)



### Downloader (works with Control-File)

[Working Example from Tests](https://github.com/freiheit-com/fuava_simplebatch/blob/master/core/src/test/java/com/freiheit/fuava/simplebatch/fsjobs/downloader/CtlDownloaderTest.java)

This is a ready-made Job Builder for a downloader which persists the fetched items in a batch file, meaning that multiple downloaded items are persisted together. 
This implementation will create control files as well.
Those files will later be processed with an importer for which an implementation exists as well (see below).

```java
final CtlDownloaderJob<Id, ?> downloader =
     new CtlDownloaderJob.BatchFileWritingBuilder<Id, String>()

        // download dir, (you can change control file ending here as well, if needed)
        .setConfiguration( new CtlDownloaderJob.ConfigurationWithPlaceholderImpl()
            .setDownloadDirPath("/tmp/foo/downloaded") 
        )

        // Fetch ids of the data to be downloaded, will be used by the 
        // downloader to fetch the data
        .setIdsFetcher( Fetchers....)
        
        // 100 items of the input fetcher will be downloaded in a batch, 
        // and also persisted in a batch. If you use the BatchFileWriterAdapter,
        // this means that those 100 items will be stored together in one file
        .setDownloaderBatchSize( 100 )
        .setDownloader( Processors....)

        // If you want to persist each downloaded item seperately, use instead:
        // downloader.setFileWriterAdapter( ... )
        .setBatchFileWriterAdapter(
          new FileWriterAdapter<List<FetchedItem<Id>>, List<String>>() {
            private final String prefix = "" + System.currentTimeMillis() + "_";
            private final AtomicLong counter = new AtomicLong();

            @Override
            public void write(
                final Writer writer, final List<String> data 
             ) throws IOException {

                final List<String> parts = ImmutableList.<String> builder()
                    .add( "<begin>" ).addAll( data ).add( "</begin>" ).build();
                    
                writer.write( Joiner.on( '\n' ).join( parts ) );
            }

            @Override
            public String getFileName( 
                     final Result<List<FetchedItem<Id>>,
                    List<String>> result 
            ) {
                return prefix + counter.incrementAndGet();
            }
        } )
        .build();
downloader.run();
```

### Importer (works with Control-File)

Example for an importer (runs after the  downloader documented above).
It imports a list of Article instances from a json file.

[Working Example from Tests](https://github.com/freiheit-com/fuava_simplebatch/blob/master/core/src/test/java/com/freiheit/fuava/simplebatch/fsjobs/importer/CtlImporterTest.java)
```java
final CtlImporterJob<Article> job = new CtlImporterJob.Builder<Article>()

    // provide settings: input directory, archive directory, etc.
    .setConfiguration( new CtlImporterJob.ConfigurationWithPlaceholderImpl()
        .setDownloadDirPath("/tmp/foo/downloaded") 
        .setArchivedDirPath("/tmp/foo/archive/%(DATE)/") 
        .setFailedDirPath("/tmp/foo/failed/%(DATE)/")
        .setProcessingDirPath("/tmp/foo/processing/")
    )

    // Read the content of a file and return it as an iterable.
    .setFileInputStreamReader((InputStream is) -> 
        new Gson().fromJson(new InputStreamReader(is), Types.listOf(Integer.class))
    )

    // the number of Article items to persist together 
    .setContentBatchSize( 100 )

    // Persist a partition of the iterator returned by the FileInputStreamReader. 
    // The maximum size of the partition is set to ContentBatchSize above.
    // Will be called repeatedly until the iterator has no more items.
    .setContentProcessor(

        // If apply of the given function fails, it will be called again 
        // with singleton lists of the given items, isolating the failed
        // items.
        Processors.retryableBatchedFunction(
            new Function<List<Article>, List<Article>>() {
                @Override
                public List<Article> apply(List<Article> data) {
                    // Store data in database
                    // If you work with transactions, then you must open and
                    // commit or rollback the transaction here.
                    db.storeAll(data);
                    return data;
                }
            }
        )
    )
    .build();

// Run the job. This reads the control files from download directory, 
// moves the control  file (and its data file) to the processing dir.
// Then it reads the data files, persists the content with the given function and
// finally moves the file (together with its control file) to the archive 
// directory, or the failed directory if all items in the file failed
ResultStatistics result = job.run();
int numFailedFiles = result.getProcessingCounts().getError();
int numProcessedFiles = result.getProcessingCounts().getSuccess();

```

If you implement a command line tool, you should call `CtlImporterJobMain.exec( fileProcessingJob )` 
instead of `fileProcessingJob.run()`. This will lead to statistics on the command line, and `System.exit(-1)` for completely failing jobs.


To collect the statistics for the processed content (i. e. for storing your articles to the database), 
you could register a `ProcessingResultListener`:

```java
job.addContentProcessingListener(new ProcessingResultListener<Article, Article>() {
    private Counts.Builder counter;
    private String filename;
    @Override
    public void onBeforeRun(String filename) {
         // the name of the proccessed file will be provided here
         this.filename = filename;
         counter = Counts.builder();
    }

    @Override
    public void onAfterRun() {
         Counts counts = counter.build();
         System.out.println("Results for file: " + filename);
         System.out.println("Errors: " + counts.getError());
         System.out.println("Successes: " + counts.getError());
    }

    @Override
    public void onProcessingResult(Result<FetchedItem<Article>,?> result) {
        // Will be called for each item after it was stored in the database. 
        counter.count(result);
    }
})

```

Note that there are convenience Implementations for those loggers available, for example `ItemProgressLoggingListener` and `BatchStatisticsLoggingListener`.

### SFTP tools

A library of common remote access tools with the focus on SFTP is provided.
It aims to generalize Downloader Job setups,which is especially helpful in the case of multiple Downloader Jobs being executed in multiple services at the same time.

Customized fetcher, processor and adapter are provided for a specific use case.
That is that a file containing the same data structure is uploaded on a regular basis on the remote system and one only wants to download the latest file identified by a timestamp in the file name.
All other (older) files are moved on the SFTP server to a predefined directory for skipped files.

The fetcher extracts the timestamp of each file from the file name and moves all files except the newest one to a predefined directory for skipped files.

The processor moves the file after downloading to a predefined directory for archived files.

The adapter writes the file while providing logging about the progress.

Example Usage:

```java
final SftpClient client = new SftpClient( 
        "sftp.somewhere.org", 22, "my_user", "secret" 
    );

final BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> downloaderJob =
    SftpDownloaderJob.makeDownloaderJob(
            new CtlDownloaderJob.ConfigurationImpl()
               .setDownloadDirPath( "/opt/downloads" ),
            client,
            new RemoteConfigurationWithPlaceholderImpl(
                "/incoming", "/downloading", "/skipped/%(DATE)", "/archived/%(DATE)" 
            ),
            new FileType( 
                "AnalyticsSystem", "_RequestDetails_{1}", ".csv", ".ok" 
            ) 
        );
BatchJobMain.exec( downloaderJob );

client.disconnect();
```

## Changes
### 1.0.0 (2021-02-03)
 - Removed Guava as a dependency completely from both the core and the sftplib.
 - Functional interfaces such as Function and Supplier now come from `java.util.function` instead from Guava.
   When upgrading they need to be replaced in the codebase.
 - `TimeLoggingProcessor.Counts` now uses the type `long` for counting instead of `int`.

### 0.7.0 (2016-04-20)
 - Changed Implementation of Concurrent processing: The fetcher is not drained so quickly any more, this should avoid OOM with lazy fetchers and large datasets.

### 0.6.12 (2016-04-08)
 - IMPORTANT Bugfix: When using parallel processing in CtlImporter, we sometimes did not wait correctly, leaving all files in processing folders.

### 0.6.8 (2016-01-22)
 - Improved Logging of Job Performance
 - Bugfix: Relative Paths did not work
 - Number of threads can be controlled for parallel processing

### 0.6.3 (2016-01-15)
 - Implemented better restart behaviour: Files belonging to the same "instanceId" will be picked up from processing dir when a CtlImporter Job starts
 - Added some strategies for subdir creation
 - Introduced System Properties to control some aspects of simplebatch (atomic file moves, instanceId, subdir strategy, parallel processing)

### 0.6.0 - 0.6.2 (2016-01-14)
 - TimeLogging will be used by default for CtlDownloader, CtlImporter and SftpDownloader, improved logging information
 - Introduced a new option for parallel processing
 - Breaking: usage of Java Path in api
 - Filenames in Downloader may not include relative directories, those will be created if needed and preserved by the CtlImporter

### 0.5.11 (2016-01-13)
 - New TimeLoggingProcessor, will unwrap composed processors and log the times per stage

### 0.5.10 (2016-01-13)
 - Fixed FileNotFound exception when a failed file was written

### 0.5.9 (2016-01-08)
 - New base classes for implementing retryable batch processors and simple single item functions  

### 0.5.8 (2016-01-06)
 - Support for providing an (optional) identifier for each processed item, which will be used for logging

### 0.5.5 (2015-12-22)
 - Fix: Include more error messages in JSON Log files
 - Polishing: fixed findbugs error in tests
 - Polishing: Processor generic parameter names improved
 - Add: BatchJob.Builder can handle Lists of listeners
 - Fix: Fail more verbose if a successful result contains null output value, improve fetcher to be more robust in this case
 
### 0.5.4 (2015-12-18)
 - Change: BatchJobMain.exec never calls System.exit but returns an exit Code you need to call System.exit yourself
 - Add: new Processor in Processors that helps with keeping the type signature of the original type.

### 0.5.3 (2015-12-14)
 - Fix: Include Messages when logging failed results

### 0.5.2 (2015-12-11)
 - Fix: NPE related to log files

### 0.5.1 (2015-12-08)
 - Add: Chaining of processors via "then"

### 0.5.0 (2015-12-01) 
 - Fixed: Copyright Headers appropriate for Apache License
 - Change: CtlImporter and CtlDownloader now create a file ".log" with structured processing state information

### 0.4.7 (2015-11-20)
 - Fixed: Empty Control Files are now treated as failed results of fetching and do not abort the entire process any more

### 0.4.6 (2015-11-16)
 - Fixed: ResultStateKeepingTransformerImpl lost the original output value
 - Add: ContentListener can be created per file in Importer Jobs

### 0.4.5 (2015-10-21)
 - Add: new Baseclass for very powerfull http paging fetchers that allow for parse errors in each fetched item 

### 0.4.4 (2015-10-21)
 - Add: new Sftp file moving fetcher baseclass which allows to implement fetchers that download the most current file (and move the rest) for multiple file types. 

### 0.4.3 (2015-10-19)
 - Fixed: Default Implementation of RemoteConfiguration had a non-standard name.
 - Fixed: Date was automatically added to some paths without users of the library having control over it. Replaced by convenient placeholder-supporting baseclasses.

### 0.4.2 (2015-10-14)
 - More forgiving code in SFTP library: configured paths may or may not end with a slash, processing dir will be created if needed

### 0.4.1 (2015-10-13)
 - Introduced SFTP library to provide remote system tools that make setting up and executing Downloader Job easier and more structured.
 - Provided customized fetcher, adapter and processor for the Downloader Job as well as several SFTP settings for the purpose of downloading the newest files, defined by a pattern,  in a given directory on an SFTP server while logging the progress.

### 0.3.9 (2015-10-01)
 - Fixed: FileMovingPersistence changed state of result item from failed to success
 - Fixed: FileMovingPersistence did not move files to date-named directories

### 0.3.8 (2015-09-18)
 - Fixed: Directory Fetcher did not sort filenames, causing behaviour to be unpredictable. Now, files are sorted by name so you should ensure that for example the timestamp or serial number of your file is a sortable part of your filename.

### 0.3.7 (2015-08-26)
 - Fixed NPE in DirectoryFetcher
 - Exposed more powerful API in Downloader Job

### 0.3.6 (2015-08-25)
 - Further improvements to BatchStatistics logging

### 0.3.5 (2015-08-25)
 - Improved Logging: Format of logging for BatchStatistics changed, included more infos
 - Improved Logging: Log info, if processing a list failed and singleton lists are retried
 - Improved documentation for Processor interface
 - gradlew should work now 

### 0.3.4 (2015-07-28)
 - Fixed HttpPager: did not process data of last page - if the very first page was less than page size, it did not return any data
