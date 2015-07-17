package com.freiheit.fuava.simplebatch.http;


public interface HttpDownloaderSettings<Input> extends RequestSettings {
	String createFetchUrl( Input input );
}