package com.freiheit.fuava.simplebatch.http;


public interface ByIdRequestSettings<Id> extends RequestSettings {
	String createFetchUrl( Id id );
}