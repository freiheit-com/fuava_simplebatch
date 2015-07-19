package com.freiheit.fuava.simplebatch.fsjobs.downloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.MapBasedBatchDownloader;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob.ConfigurationImpl;
import com.freiheit.fuava.simplebatch.processor.BatchProcessorResult;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.FileWriterAdapter;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

@Test
public class CtlDownloaderTest {

    public static final String TEST_DIR_BASE = "/tmp/fuava-simplebatch-test/";
    public static final String TEST_DIR_DOWNLOADS = TEST_DIR_BASE + "/downloads/";

    public static <Input, Output> CtlDownloaderJob.BatchFileWritingBuilder<Input, Output> newTestDownloaderBuilder() {
        return new CtlDownloaderJob.BatchFileWritingBuilder<Input, Output>()
                .setConfiguration(new ConfigurationImpl().setDownloadDirPath(TEST_DIR_DOWNLOADS));
    }

    @Test
    public void testBatchPersistence() throws FileNotFoundException, IOException {

        final String targetFileName = "batch";
        final File expected = new File(TEST_DIR_DOWNLOADS, targetFileName + "_1");
        if (expected.exists()) {
            expected.delete();
        }
        Assert.assertFalse(expected.exists(), "File was not deleted ");

        final Map<Integer, String> data = new LinkedHashMap<Integer, String>();
        data.put(1, "eins");
        data.put(2, "zwei");
        data.put(3, "drie");
        data.put(4, "vier");
        data.put(5, "fünf");
        data.put(6, "sechs");
        final CtlDownloaderJob.BatchFileWritingBuilder<Integer, String> builder = newTestDownloaderBuilder();
        final CtlDownloaderJob<Integer, BatchProcessorResult<ControlFilePersistenceOutputInfo>> downloader = builder
                .setDownloaderBatchSize(100)
                // Fetch ids of the data to be downloaded, will be used by the downloader to fetch the data
                .setIdsFetcher(Fetchers.iterable(data.keySet()))
                .setDownloader(Processors.retryableBatchedFunction(new MapBasedBatchDownloader<Integer, String>(data)))
                .setBatchFileWriterAdapter(new FileWriterAdapter<List<FetchedItem<Integer>>, List<String>>() {
                    private final String prefix = targetFileName + "_";
                    private final AtomicLong counter = new AtomicLong();
                    @Override
                    public void write(Writer writer, List<String> data) throws IOException {
                        String string = Joiner.on('\n').join(data);
                        writer.write(string);
                    }

                    @Override
                    public String getFileName(Result<List<FetchedItem<Integer>>, List<String>> result) {
                        return prefix + counter.incrementAndGet();
                    }
                })
                .build();

        ResultStatistics results = downloader.run();
        Assert.assertTrue(results.isAllSuccess());
        Assert.assertFalse(results.isAllFailed());

        Assert.assertTrue(expected.exists(), "batch file was not created");

        ImmutableList.Builder<String> resultsBuilder = ImmutableList.builder();
        try (BufferedReader reader = new BufferedReader(new FileReader(expected))) {
            while (reader.ready()) {
                resultsBuilder.add(reader.readLine());
            }
        }
        ImmutableList<String> resultsList = resultsBuilder.build();
        Assert.assertEquals(resultsList, data.values());
    }
}


