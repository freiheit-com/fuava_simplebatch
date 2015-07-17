package com.freiheit.fuava.simplebatch.fetch;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.http.HttpFetcher;
import com.freiheit.fuava.simplebatch.http.HttpPagingFetcher;
import com.freiheit.fuava.simplebatch.http.PagingRequestSettings;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class Fetchers {

	/**
	 * Uses the given iterable to provide the data that will be passed on to the processing stage, treating each item as successful.
	 */
	public static final <T> Fetcher<T> iterable(Iterable<T> iterable) {
		return new SuppliedIterableFetcher<T>(Suppliers.ofInstance(iterable));
	}
	
	/**
	 * Uses the given fetcher to get an  iterable that provides the data that will be passed on to the processing stage, treating each item as successful.
	 */
	public static final <T> Fetcher<T> supplied(Supplier<Iterable<T>> supplier) {
		return new SuppliedIterableFetcher<T>(supplier);
	}

	/**
	 * A Fetcher that uses an http request to retrieve the data to process.
	 */
	public static <Item> Fetcher<Item> httpFetcher(
			final HttpClient client, 
			final String uri,
			
    		final Map<String, String> headers,
    		final Function<InputStream, Iterable<Item>> converter
	) {
		HttpFetcher httpFetcher = new HttpFetcher(client);
		return new SuppliedIterableFetcher<Item>(new Supplier<Iterable<Item>>() {

			@Override
			public Iterable<Item> get() {
				return httpFetcher.fetch(converter, uri, headers);
			}
		});
	}

	/**
	 * Creates a Fetcher which fetches the Items lazily via http, always requesting a page of the data
	 * and transparently continuing to the next page. 
	 */
	public static <Item> Fetcher<Item> httpPagingFetcher(
			final HttpClient client, 
    		final PagingRequestSettings<Iterable<Item>> settings,
    		final Function<InputStream, Iterable<Item>> converter,
    		int initialFrom,
    		int pageSize
	) {
		return new HttpPagingFetcher<Item>(client, settings, converter, initialFrom, pageSize);
		
	}

	/**
	 * Iterates over all files in the given directory that end with the specified fileEnding and converts them with the given function.
	 */
	public static <T> Fetcher<T> folderFetcher(String dirName, String fileEnding, Function<File, T> fileFunction) {
		return supplied(new DirectoryFileFetcher<T>(dirName, fileEnding, fileFunction));
	}

	/**
	 * Iterates over all files in the given directory that end with the specified fileEnding.
	 */
	public static Fetcher<File> folderFetcher(String dirName, String fileEnding) {
		return supplied(new DirectoryFileFetcher<File>(dirName, fileEnding, Functions.<File>identity()));
	}

}

