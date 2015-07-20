package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.MapBasedBatchDownloader;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob.ConfigurationImpl;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.processor.StringFileWriterAdapter;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.util.FileUtils;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Test
public class CtlImporterTest {

    @Test
    public void testDownloadAndImport() throws FileNotFoundException, IOException {
        final String testDirBase = "/tmp/CtlImporterTest/" + System.currentTimeMillis() + "/";
        final String testDirDownloads = testDirBase + "/downloads/";
        final String testDirProcessing = testDirBase + "/processing/";
        final String testDirArchive = testDirBase + "/archive/";
        final String testDirFails = testDirBase + "/fails/";

        final Map<Integer, String> data = ImmutableMap.of(
                1, "eins",
                2, "zwei",
                3, "drie",
                4, "vier"
                );

        final AtomicLong counter = new AtomicLong();
        ResultStatistics downloadResults = new CtlDownloaderJob.Builder<Integer, String>()
                .setConfiguration(new ConfigurationImpl().setDownloadDirPath(testDirDownloads))
                .setDownloaderBatchSize(100)
                .setIdsFetcher(Fetchers.iterable(data.keySet()))
                .setDownloader(Processors.retryableBatchedFunction(new MapBasedBatchDownloader<Integer, String>(data)))
                .setFileWriterAdapter(new StringFileWriterAdapter<FetchedItem<Integer>>() {
                    @Override
                    public String getFileName(Result<FetchedItem<Integer>, String> result) {
                        return counter.incrementAndGet() + ".tmp";
                    }
                })
                .build()
                .run();

        Assert.assertTrue(downloadResults.isAllSuccess());
        Assert.assertFalse(downloadResults.isAllFailed());


        final List<String> importedLines = new ArrayList<String>();

        ResultStatistics importResults = new CtlImporterJob.Builder<String>()
                .setConfiguration(
                        new CtlImporterJob.ConfigurationImpl()
                        .setArchivedDirPath(testDirArchive)
                        .setDownloadDirPath(testDirDownloads)
                        .setFailedDirPath(testDirFails)
                        .setProcessingDirPath(testDirProcessing)
                        )
                        .setFileInputStreamReader(new Function<InputStream, Iterable<String>>() {

                            @Override
                            public Iterable<String> apply(InputStream input) {
                                try {
                                    try (BufferedReader ir = new BufferedReader(new InputStreamReader(input))) {
                                        return ImmutableList.of(ir.readLine());
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        })
                        .setContentProcessor(Processors.retryableBatchedFunction(new Function<List<String>, List<String>>() {

                            @Override
                            public List<String> apply(List<String> input) {
                                importedLines.addAll(input);
                                return input;
                            }
                        }))
                        .build()
                        .run();
        Assert.assertTrue(importResults.isAllSuccess());
        Assert.assertFalse(importResults.isAllFailed());


        // ignore ordering, because reading of the input files currently is not ordered. 
        Assert.assertEquals(ImmutableSet.copyOf(importedLines), ImmutableSet.copyOf(data.values()));
        Assert.assertTrue(importedLines.size() == 4);

        File baseDir = new File(testDirBase);
        FileUtils.deleteDirectoryRecursively(baseDir);
    }
}


