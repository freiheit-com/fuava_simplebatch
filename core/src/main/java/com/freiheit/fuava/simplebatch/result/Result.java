package com.freiheit.fuava.simplebatch.result;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class Result<Input, Output> {
	private static final Logger LOG = LoggerFactory.getLogger( Result.class );
	
	public static final class Builder<Input, Output> {
	    private Input input;
	    private Output output;
	    private List<String> failureMessages;
	    private List<String> warningMessages;
	    private List<Throwable> throwables;
		
	    
	    public Builder<Input, Output> withInput(Input input) {
	    	this.input = input;
	    	return this;
	    }

	    public Builder<Input, Output> withOutput(Output output) {
	    	this.output = output;
	    	return this;
	    }

	    public Builder<Input, Output> withWarningMessages(Iterable<String> msgs) {
	    	for (String msg: msgs) {
	    		withWarningMessage(msg);
	    	}
	    	return this;
	    }
	    
	    public Builder<Input, Output> withWarningMessage(String msg) {
	    	if (warningMessages == null) {
	    		warningMessages = new ArrayList<String>();
	    	}
	    	warningMessages.add(msg);
	    	return this;
	    }

	    public Builder<Input, Output> withFailureMessage(String msg) {
	    	if (failureMessages == null) {
	    		failureMessages = new ArrayList<String>();
	    	}
	    	failureMessages.add(msg);
	    	return this;
	    }
	    
	    public Builder<Input, Output> withFailureMessages(Iterable<String> msgs) {
	    	for (String msg: msgs) {
	    		withFailureMessage(msg);
	    	}
	    	return this;
	    }

	    private Builder<Input, Output> withThrowable(Throwable t) {
	    	if (throwables == null) {
	    		throwables = new ArrayList<Throwable>();
	    	}
	    	throwables.add(t);
	    	return this;
	    }

	    public Builder<Input, Output> withThrowables(Iterable<Throwable> throwables) {
	    	for (Throwable t: throwables) {
	    		withThrowable(t);
	    	}
	    	return this;
	    }

	    private Result<Input, Output> build(boolean failed) {
	    	return new Result<Input, Output>(input, output, failed, warningMessages, failureMessages, throwables);
	    }

	    public Result<Input, Output> failed() {
	    	return build(true);
	    }
	    
	    public Result<Input, Output> failed(Throwable t) {
	    	if (t != null) {
	    		String msg = input + " - " + (failureMessages == null ? "" : Joiner.on(" | ").join(failureMessages));
	    		LOG.error(msg, t);
	    	}
	    	withThrowable(t);
	    	return build(true);
	    }

	    public Result<Input, Output> success() {
	    	return build(false);
	    }

	}
	
    private final Input input;
    private final Output output;
    private final boolean failed;
    private final List<String> failureMessages;
    private final List<String> warningMessages;
    private final List<Throwable> throwables;

    private Result( 
    		Input input, 
    		Output output, 
    		boolean failed, 
    		Iterable<String> warningMessages, 
    		Iterable<String> failureMessages, 
    		Iterable<Throwable> ts
	) {
        this.input = input;
        this.output = output;
        this.failed = failed;
        this.failureMessages = failureMessages == null ? ImmutableList.of(): ImmutableList.copyOf(failureMessages);
        this.warningMessages = warningMessages == null ? ImmutableList.of(): ImmutableList.copyOf(warningMessages);
        this.throwables = ts == null ? ImmutableList.of() : ImmutableList.copyOf(ts);
    }

    public static final <I, O> Builder<I, O> builder() {
    	return new Builder<I, O>();
    }
    
    public static final <I, O> Builder<I, O> builder(Result<I, ?> orig) {
		return new Builder<I, O>()
				.withInput(orig.getInput())
				.withThrowables(orig.getThrowables())
				.withWarningMessages(orig.getWarningMessages())
				.withFailureMessages(orig.getFailureMessages());
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

    public Iterable<String> getFailureMessages() {
		return failureMessages;
	}
    
    public Iterable<String> getWarningMessages() {
		return warningMessages;
	}

    public Iterable<Throwable> getThrowables() {
		return throwables;
	}

    public static <I, O> Result<I, O> success( I input, O output ) {
        return success( input, output, ImmutableList.of() );
    }

    public static <I, O> Result<I, O> success( I input, O output, Iterable<String> warnings ) {
        return new Result<I, O>( input, output, false, warnings, ImmutableList.of(), null );
    }

    public static <I, O> Result<I, O> failed( I id, Throwable t ) {
        return failed( id, ImmutableList.of(), t );
    }

    public static <I, O> Result<I, O> failed( I id, String failureMessage, Throwable t ) {
        return failed( id , failureMessage == null ? ImmutableList.<String>of() : ImmutableList.of(failureMessage), t);
    }

    public static <I, O> Result<I, O> failed( I id, String failureMessage) {
        return failed( id , failureMessage == null ? ImmutableList.<String>of() : ImmutableList.of(failureMessage), null);
    }

    public static <I, O> Result<I, O> failed( I id, Iterable<String> failureMessages, Throwable t ) {
    	if (t != null) {
    		String msg = id + " - " + (failureMessages == null ? "" : Joiner.on(" | ").join(failureMessages));
    		LOG.error(msg, t);
    	}
    	return new Result<I, O>( id , null, true, ImmutableList.of(), failureMessages, t == null ? ImmutableList.<Throwable>of(): ImmutableList.of(t));
    }

    public static <I, O> Result<I, O> failed( I id, Iterable<String> failureMessages ) {
        return failed( id , failureMessages, null);
    }
    
    @Override
    public String toString() {
    	if (this.failed) {
    		return "FAIL: " + input + " [" + failureMessages.size() + " messages]";
    	}
    	return "SUCCESS: " + input + " => " + output + " [" + warningMessages.size() + " messages]";
    }

}