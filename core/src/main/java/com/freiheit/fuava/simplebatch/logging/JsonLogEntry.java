package com.freiheit.fuava.simplebatch.logging;

import com.google.gson.Gson;

public class JsonLogEntry {

    private static final Gson gson = new Gson();
    
    private String context;
    private String event;
    private Boolean success;
    private String input;
    private Integer number;
    private long time;

    public JsonLogEntry( String context, String event, Boolean success, Integer number, String input ) {
        this.context = context;
        this.event = event;
        this.success = success;
        this.input = input;
        this.number = number;                 
        this.time = System.currentTimeMillis();
    }        

    public String getContext() {
        return context;
    }

    public String getEvent() {
        return event;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getInput() {
        return input;
    }

    public long getNumber() {
        return number;
    }

    public long getTime() {
        return time;
    }    
    
    public String toJson() {
        return gson.toJson( this );
    }        
    
    public static JsonLogEntry fromJson(String json) {
        return gson.fromJson( json, JsonLogEntry.class );
    }
}
