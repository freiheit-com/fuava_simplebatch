package com.freiheit.fuava.simplebatch.logging;

public class JsonLogEntry {
    
    private final String context;
    private final String event;
    private final Boolean success;
    private final String input;
    private final Integer number;
    private final long time;

    public JsonLogEntry( final String context, final String event, final Boolean success, final Integer number, final String input ) {
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
    
}
