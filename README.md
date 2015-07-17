# Fuava SimpleBatch
A Java library that aims to make implementation of simple, fast and failsafe batch processing jobs nearly trivial.

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

## General BatchJob
The general pattern for implementing a batch job (no matter wether files are used or not) is:


```java
final BatchJob<Input, ProcessedData> job = 
    new BatchJob.Builder<Input, ProcessedData>()
        .setFetcher( Fetchers....)
        .setProcessingBatchSize( 100 )
        .setProcessor( Processors....)
        .setPersistence( Persistences... )
        .build();
downloader.run();
```


[BatchJob.java](https://github.com/freiheit-com/fuava_simplebatch/blob/master/core/src/main/java/com/freiheit/fuava/simplebatch/BatchJob.java)



## Importer (works with Control-File)

Example for an importer (runs after the  downloader documented below).
It imports a list of Article instances from a json file.
```java
final CtlImporterJob<Article> job = new CtlImporterJob.Builder<Article>()

    // provide settings: input directory, archive directory, etc.
    .setConfiguration( config )

    // Read the content of a file and return it as an iterable.
    .setFileInputStreamReader((InputStream is) -> 
        new Gson().fromJson(new InputStreamReader(is), Types.listOf(Integer.class))
    )

    // the number of Article items to persist together 
    .setContentBatchSize( 100 )

    // Persist a partition of the iterator returned by the FileInputStreamReader. 
    // The maximum size of the partition is set to ContentBatchSize above.
    // Will be called repeatedly until the iterator has no more items.
    .setContentPersistence(

        // If apply of the given function fails, it will be called again 
        // with singleton lists of the given items, isolating the failed
        // items.
        Persistences.retryableBatchedFunction(
            new Function<List<Article>, List<Article>>() {
                @Override
                public List<Integer> apply(List<Article> data) {
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
int numFailedFiles = result.getPersistCounts().getError();
int numProcessedFiles = result.getPersistCounts().getSuccess();

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
    public void onPersistResult(Result<Article,?> result) {
        // Will be called for each item after it was stored in the database. 
        counter.count(result);
    }
})

```

Note that there are convenience Implementations for those loggers available, for example `ItemProgressLoggingListener` and `BatchStatisticsLoggingListener`.


## Downloader (works with Control-File)
Example for a downloader, that persists the fetched items in a batch file, meaning that multiple downloaded items are persisted together.

```java
final CtlDownloaderJob<ClipboardArticleId, String> downloader = new CtlDownloaderJob.Builder<ClipboardArticleId, String>()
        .setConfiguration( config )
        .setDownloaderBatchSize( 100 )
        // Fetch ids of the data to be downloaded, will be used by the downloader to fetch the data
        .setIdsFetcher( Fetchers....)
        .setDownloader( Processors....)

        .setBatchFileWriterAdapter( new PersistenceAdapter<List<ClipboardArticleId>, List<String>>() {
            private final String prefix = "" + System.currentTimeMillis() + "_";
            private final AtomicLong counter = new AtomicLong();

            @Override
            public void write( final Writer writer, final List<String> data ) throws IOException {

                final ImmutableList.Builder<String> builder = ImmutableList.<String> builder();
                builder.add( "<begin>" );
                builder.addAll( data );
                builder.add( "</begin>" );
                final String string = Joiner.on( '\n' ).join( builder.build() );
                writer.write( string );
            }

            @Override
            public String getFileName( final Result<List<ClipboardArticleId>, List<String>> result ) {
                return prefix + counter.incrementAndGet();
            }
        } )
        .build();
downloader.run();
```
Example for a 'normal' downloader:

```java
final CtlDownloaderJob<ClipboardArticleId, String> downloader = new CtlDownloaderJob.Builder<ClipboardArticleId, String>()
        .setConfiguration( config )
        .setDownloaderBatchSize( 100 )
        // Fetch ids of the data to be downloaded, will be used by the downloader to fetch the data
        .setIdsFetcher( Fetchers....)
        .setDownloader( Processors....)
        .setFileWriterAdapter( ... )
        .build();
downloader.run();
```

## Downloader (works with Control-File)
Example for a downloader, that persists the fetched items in a batch file, meaning that multiple downloaded items are persisted together.

```java
final CtlDownloaderJob<ClipboardArticleId, String> downloader = new CtlDownloaderJob.Builder<ClipboardArticleId, String>()
        .setConfiguration( config )
        .setDownloaderBatchSize( 100 )
        // Fetch ids of the data to be downloaded, will be used by the downloader to fetch the data
        .setIdsFetcher( Fetchers....)
        .setDownloader( Processors....)

        .setBatchFileWriterAdapter( new PersistenceAdapter<List<ClipboardArticleId>, List<String>>() {
            private final String prefix = "" + System.currentTimeMillis() + "_";
            private final AtomicLong counter = new AtomicLong();

            @Override
            public void write( final Writer writer, final List<String> data ) throws IOException {

                final ImmutableList.Builder<String> builder = ImmutableList.<String> builder();
                builder.add( "<begin>" );
                builder.addAll( data );
                builder.add( "</begin>" );
                final String string = Joiner.on( '\n' ).join( builder.build() );
                writer.write( string );
            }

            @Override
            public String getFileName( final Result<List<ClipboardArticleId>, List<String>> result ) {
                return prefix + counter.incrementAndGet();
            }
        } )
        .build();
downloader.run();
```
Example for a 'normal' downloader:

```java
final CtlDownloaderJob<Id, String> downloader = new CtlDownloaderJob.Builder<Id, String>()
        .setConfiguration( config )
        .setDownloaderBatchSize( 100 )
        // Fetch ids of the data to be downloaded, will be used by the downloader to fetch the data
        .setIdsFetcher( Fetchers....)
        .setDownloader( Processors....)
        .setFileWriterAdapter( ... )
        .build();
downloader.run();
```

