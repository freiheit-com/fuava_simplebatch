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

import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob.ConfigurationImpl;
import com.freiheit.fuava.simplebatch.persist.PersistenceAdapter;
import com.freiheit.fuava.simplebatch.process.Processors;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

@Test
public class CtlDownloaderTest {

	private static final String TMP_FUAVA_SIMPLEBATCH_TEST = "/tmp/fuava-simplebatch-test/";
	private <Input, Output> CtlDownloaderJob.Builder<Input, Output> newTestDownloaderBuilder() {
		return new CtlDownloaderJob.Builder<Input, Output>()
				.setConfiguration(new ConfigurationImpl().setDownloadDirPath(TMP_FUAVA_SIMPLEBATCH_TEST));
	}
	
	private static final class MapBasedBatchDownloader<I, O> implements Function<List<I>, List<O>>  {
		private final Map<I, O> map;
		public MapBasedBatchDownloader(Map<I, O> map) {
			this.map = map;
		}
		@Override
		public List<O> apply(List<I> arg0) {
			ImmutableList.Builder<O> b = ImmutableList.builder();
			for (I i: arg0) {
				b.add(map.get(i));
			}
			return b.build();
		}
		
	}
	@Test
	public void testBatchPersistence() throws FileNotFoundException, IOException {
		
		final String targetFileName = "batch";
		final File expected = new File(TMP_FUAVA_SIMPLEBATCH_TEST, targetFileName + "_1");
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
		final CtlDownloaderJob.Builder<Integer, String> builder = newTestDownloaderBuilder();
		final CtlDownloaderJob<Integer, String> downloader = builder
				.setDownloaderBatchSize(100)
                // Fetch ids of the data to be downloaded, will be used by the downloader to fetch the data
                .setIdsFetcher(Fetchers.iterable(data.keySet()))
                .setDownloader(Processors.retryableBatch(new MapBasedBatchDownloader<Integer, String>(data)))
                .setBatchFileWriterAdapter(new PersistenceAdapter<List<Integer>, List<String>>() {
					private final String prefix = targetFileName + "_";
					private final AtomicLong counter = new AtomicLong();
					@Override
					public void write(Writer writer, List<String> data) throws IOException {
						String string = Joiner.on('\n').join(data);
						writer.write(string);
					}
					
					@Override
					public String getFileName(Result<List<Integer>, List<String>> result) {
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


