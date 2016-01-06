package com.freiheit.fuava.simplebatch.logging;

import java.util.List;

public class JsonLogEntry {
    
    private final String context;
    private final String event;
    private final Boolean success;
    private final String input;
    private final List<String> messages;
    private final Integer number;
    private final long time;
    private final String idString;

    public JsonLogEntry(
            final String context,
            final String event,
            final Boolean success,
            final Integer number,
            final String input,
            final List<String> messages,
            final String idString ) {
        this.context = context;
        this.event = event;
        this.success = success;
        this.input = input;
        this.number = number;   
        this.messages = messages;
        this.idString = idString;
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
    
    public List<String> getMessages() {
        return messages;
    }

    public String getIdString() {
        return idString;
    }

}
