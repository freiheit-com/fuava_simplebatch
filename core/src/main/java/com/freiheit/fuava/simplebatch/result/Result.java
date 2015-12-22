/**
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.simplebatch.result;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class Result<Input, Output> {
    private static final Logger LOG = LoggerFactory.getLogger( Result.class );

    public static final class Builder<Input, Output> {
        private Input input;
        private Output output;
        private List<String> failureMessages;
        private List<String> warningMessages;
        private List<Throwable> throwables;

        public Builder<Input, Output> withInput( final Input input ) {
            this.input = input;
            return this;
        }

        public Builder<Input, Output> withOutput( final Output output ) {
            this.output = output;
            return this;
        }

        public Builder<Input, Output> withWarningMessages( final Iterable<String> msgs ) {
            for ( final String msg : msgs ) {
                withWarningMessage( msg );
            }
            return this;
        }

        public Builder<Input, Output> withWarningMessage( final String msg ) {
            if ( warningMessages == null ) {
                warningMessages = new ArrayList<String>();
            }
            warningMessages.add( msg );
            return this;
        }

        public Builder<Input, Output> withFailureMessage( final String msg ) {
            if ( failureMessages == null ) {
                failureMessages = new ArrayList<String>();
            }
            
            failureMessages.add( msg == null ? "Failure Message is null!" : msg );
            return this;
        }

        public Builder<Input, Output> withFailureMessages( final Iterable<String> msgs ) {
            for ( final String msg : msgs ) {
                withFailureMessage( msg );
            }
            return this;
        }

        private Builder<Input, Output> withThrowable( final Throwable t ) {
            if ( throwables == null ) {
                throwables = new ArrayList<Throwable>();
            }
            if ( t != null ) {
                throwables.add( t );
            }
            return this;
        }

        public Builder<Input, Output> withThrowables( final Iterable<Throwable> throwables ) {
            for ( final Throwable t : throwables ) {
                withThrowable( t );
            }
            return this;
        }

        private Result<Input, Output> build( final boolean failed ) {
            return new Result<Input, Output>( input, output, failed, warningMessages, failureMessages, throwables );
        }

        public Result<Input, Output> failed() {
            return build( true );
        }

        public Result<Input, Output> failed( final Throwable t ) {
            if ( t != null ) {
                final String msg = input + " - " + ( failureMessages == null
                    ? ""
                    : Joiner.on( " | " ).join( failureMessages ) );
                LOG.error( msg, t );
                withFailureMessage( t.getMessage() );
                withThrowable( t );
            }
            return build( true );
        }

        public Result<Input, Output> success() {
            return build( false );
        }

    }

    private final Input input;
    private final Output output;
    private final boolean failed;
    private final List<String> failureMessages;
    private final List<String> warningMessages;
    private final List<Throwable> throwables;

    private Result(
            final Input input,
            final Output output,
            final boolean failed,
            final Iterable<String> warningMessages,
            final Iterable<String> failureMessages,
            final Iterable<Throwable> ts ) {
        if ( !failed && output == null ) {
            throw new IllegalArgumentException( "Successful results always must contain a result" );
        }
        this.input = input;
        this.output = output;
        this.failed = failed;
        this.failureMessages = failureMessages == null
            ? ImmutableList.of()
            : ImmutableList.copyOf( failureMessages );
        this.warningMessages = warningMessages == null
            ? ImmutableList.of()
            : ImmutableList.copyOf( warningMessages );
        this.throwables = ts == null
            ? ImmutableList.of()
            : ImmutableList.copyOf( ts );
    }

    public static final <I, O> Builder<I, O> builder() {
        return new Builder<I, O>();
    }

    public static final <I, O> Builder<I, O> builder( final Result<I, ?> orig ) {
        return builder( orig, orig.getInput() );
    }

    public static final <I, O> Builder<I, O> builder( final Result<?, ?> orig, final I input ) {
        return new Builder<I, O>()
                .withInput( input )
                .withThrowables( orig.getThrowables() )
                .withWarningMessages( orig.getWarningMessages() )
                .withFailureMessages( orig.getFailureMessages() );
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

    public List<String> getAllMessages() {
        return ImmutableList.copyOf( Iterables.concat( getWarningMessages(), getFailureMessages() ) );
    }

    public static <I, O> Result<I, O> success( final I input, final O output ) {
        return success( input, output, ImmutableList.of() );
    }

    public static <I, O> Result<I, O> success( final I input, final O output, final Iterable<String> warnings ) {
        return new Result<I, O>( input, output, false, warnings, ImmutableList.of(), null );
    }

    public static <I, O> Result<I, O> failed( final I id, final Throwable t ) {
        return failed( id, ImmutableList.of(), t );
    }

    public static <I, O> Result<I, O> failed( final I id, final String failureMessage, final Throwable t ) {
        return failed( id, failureMessage == null
            ? ImmutableList.<String> of()
            : ImmutableList.of( failureMessage ), t );
    }

    public static <I, O> Result<I, O> failed( final I id, final String failureMessage ) {
        return failed( id, failureMessage == null
            ? ImmutableList.<String> of()
            : ImmutableList.of( failureMessage ), null );
    }

    public static <I, O> Result<I, O> failed( final I id, final Iterable<String> failureMessages, final Throwable t ) {
        if ( t != null ) {
            final String msg = id + " - " + ( failureMessages == null
                ? ""
                : Joiner.on( " | " ).join( failureMessages ) );
            LOG.error( msg, t );
        }
        return new Result<I, O>( id, null, true, ImmutableList.of(), failureMessages, t == null
            ? ImmutableList.<Throwable> of()
            : ImmutableList.of( t ) );
    }

    public static <I, O> Result<I, O> failed( final I id, final Iterable<String> failureMessages ) {
        return failed( id, failureMessages, null );
    }

    @Override
    public String toString() {
        if ( this.failed ) {
            return "FAIL: " + input + " [" + failureMessages.size() + " messages]";
        }
        return "SUCCESS: " + input + " => " + output + " [" + warningMessages.size() + " messages]";
    }

}