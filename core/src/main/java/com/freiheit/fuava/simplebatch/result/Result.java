package com.freiheit.fuava.simplebatch.result;

public class Result<Input, Output> {
	
    private final Input input;
    private final Output output;
    private final boolean failed;
    private final String failureMessage;
    private final Throwable t;

    private Result( Input input, Output output) {
        this.input = input;
        this.output = output;
        this.failed = false;
        this.failureMessage = null;
        this.t = null;
    }

    private Result( Input input, String failureMessage, Throwable t) {
        this.input = input;
        this.output = null;
        this.failed = true;
        this.failureMessage = failureMessage;
        this.t = t;
    }

    public Input getInput() {
		return input;
	}
    
    public Output getOutput() {
		return output;
	}

    public boolean isFailed() {
    	return failed;
    }

    public boolean isSuccess() {
    	return !failed;
    }

    public String getFailureMessage() {
		return failureMessage;
	}
    
    public Throwable getException() {
		return t;
	}
    
    public static <I, O> Result<I, O> success( I input, O output ) {
        return new Result<I, O>( input, output );
    }

    public static <I, O> Result<I, O> failed( I id, Throwable t ) {
        return new Result<I, O>( id, null, t );
    }

    public static <I, O> Result<I, O> failed( I id, String failureMessage, Throwable t ) {
        return new Result<I, O>( id , failureMessage, t);
    }

    public static <I, O> Result<I, O> failed( I id, String failureMessage) {
        return new Result<I, O>( id , failureMessage, null);
    }

}