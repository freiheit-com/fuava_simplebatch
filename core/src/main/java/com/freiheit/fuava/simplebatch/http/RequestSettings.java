package com.freiheit.fuava.simplebatch.http;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public interface RequestSettings {
    default Map<String, String> getRequestHeaders() {
        return ImmutableMap.<String, String> of();
    }
}