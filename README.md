# Fuava SimpleBatch
A Java library that makes implementation of simple, fast and failsafe batch processing jobs nearly trivial.

It provides many helpers and default implementations for everyday batch processing tasks.

With SimpleBatch, you can write your Jobs such that they comply to the following standards
  - Isolate Failures. If Processing or Persisting of one Item fails, everything else continues.
  - Process Batches. A lot of tasks (for example Database queries) are a lot faster if performed for multiple items at once.
  - Iterate over large Datasets. If your Input Iterable is lazy, you can process huge datasets where only the currently processed batches are kept in memory at any one time.
  - Communicate processing status. You will recieve processing statistics at the end of your job and you can register listeners to keep you informed.

We provide classes to streamline the following tasks
  - Any type of plain old BatchJob that has a data fetching, a data processing and a persistence stage.
  - Downloader / Importer pair of Command Line Tools (or jobs, if you prefer) that communicate via the filesystem. The Downloader will create control files which will then be used by the importer to read the downloaded file, move it to a processing folder, process it and in the end move it to an archive folder, or failed folder if it failed completely.
  - Simple Command line tool that perform some batch job, print statistics and fail if and only if all items failed

[Example](https://github.com/freiheit-com/fuava_simplebatch/tree/master/example)

## Downloader (works with Control-File)



## 

[BatchJob.java](https://github.com/freiheit-com/fuava_simplebatch/blob/master/core/src/main/java/com/freiheit/fuava/simplebatch/BatchJob.java)

