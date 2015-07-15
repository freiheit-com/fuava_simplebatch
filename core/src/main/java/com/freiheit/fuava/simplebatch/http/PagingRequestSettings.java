package com.freiheit.fuava.simplebatch.http;

import com.freiheit.fuava.simplebatch.fetch.PageFetchingSettings;


public interface PagingRequestSettings<T> extends RequestSettings, PageFetchingSettings<T> {
	String createFetchUri(int from, int amount);
}