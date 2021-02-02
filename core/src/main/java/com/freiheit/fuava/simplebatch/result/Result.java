/*
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

import com.freiheit.fuava.simplebatch.util.IterableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Contains the result of a fetching or processing step.
 * 
 * A result can either be successful or failed.
 * 
 * @author klas.kalass@freiheit.com
 *
 * @param <OriginalItem>
 *            The item which is considered the input for the fetching or
 *            processing step
 * @param <Output>
 *            The item which is the result of the fetching or processing step
 */
public class Result<OriginalItem, Output> {
    private static final Logger LOG = LoggerFactory.getLogger( Result.class );

    public static final class Builder<OriginalItem, Output> {
        private OriginalItem input;
        private Output output;
        private List<String> failureMessages;
        private List<String> warningMessages;
        private List<Throwable> throwables;

        public Builder<OriginalItem, Output> withInput( final OriginalItem originalItem ) {
            this.input = originalItem;
            return this;
        }

        public Builder<OriginalItem, Output> withOutput( final Output output ) {
            this.output = output;
            return this;
        }

        public Builder<OriginalItem, Output> withWarningMessages( final Iterable<String> msgs ) {
            for ( final String msg : msgs ) {
                withWarningMessage( msg );
            }
            return this;
        }

        public Builder<OriginalItem, Output> withWarningMessage( final String msg ) {
            if ( warningMessages == null ) {
                warningMessages = new ArrayList<>();
            }
            warningMessages.add( msg );
            return this;
        }

        public Builder<OriginalItem, Output> withFailureMessage( final String msg ) {
            if ( failureMessages == null ) {
                failureMessages = new ArrayList<>();
            }
            
            failureMessages.add( msg == null ? "Failure Message is null!" : msg );
            return this;
        }

        public Builder<OriginalItem, Output> withFailureMessages( final Iterable<String> msgs ) {
            for ( final String msg : msgs ) {
                withFailureMessage( msg );
            }
            return this;
        }

        private Builder<OriginalItem, Output> withThrowable( final Throwable t ) {
            if ( throwables == null ) {
                throwables = new ArrayList<>();
            }
            if ( t != null ) {
                throwables.add( t );
            }
            return this;
        }

        public Builder<OriginalItem, Output> withThrowables( final Iterable<Throwable> throwables ) {
            for ( final Throwable t : throwables ) {
                withThrowable( t );
            }
            return this;
        }

        private Result<OriginalItem, Output> build( final boolean failed ) {
            return new Result<>( input, output, failed, warningMessages, failureMessages, throwables );
        }

        public Result<OriginalItem, Output> failed() {
            return build( true );
        }

        public Result<OriginalItem, Output> failed( final Throwable t ) {
            if ( t != null ) {
                final String msg = input + " - " + ( failureMessages == null
                    ? ""
                    : String.join( " | ", failureMessages ) );
                LOG.error( msg, t );
                withFailureMessage( t.getMessage() );
                withThrowable( t );
            }
            return build( true );
        }

        public Result<OriginalItem, Output> success() {
            return build( false );
        }

    }

    private final OriginalItem input;
    private final Output output;
    private final boolean failed;
    private final List<String> failureMessages;
    private final List<String> warningMessages;
    private final List<Throwable> throwables;

    private Result(
            final OriginalItem input,
            final Output output,
            final boolean failed,
            final Iterable<String> warningMessages,
            final Iterable<String> failureMessages,
            final Iterable<Throwable> ts ) {
        if ( ts != null ) {
            for (final Throwable t : ts) {
                if (t instanceof VirtualMachineError ) {
                    // there is no way how those errors could be handled correctly.
                    throw (VirtualMachineError)t;
                }
            }
        }
        if ( !failed && output == null ) {
            throw new IllegalArgumentException( "Successful results always must contain a result" );
        }
        this.input = input;
        this.output = output;
        this.failed = failed;
        this.failureMessages = failureMessages == null
            ? Collections.emptyList()
            : IterableUtils.asList( failureMessages );
        this.warningMessages = warningMessages == null
            ? Collections.emptyList()
            : IterableUtils.asList( warningMessages );
        this.throwables = ts == null
            ? Collections.emptyList()
            : IterableUtils.asList( ts );
    }

    public static <OriginalItem, Output> Builder<OriginalItem, Output> builder() {
        return new Builder<>();
    }

    public static <OriginalItem, Output> Builder<OriginalItem, Output> builder( final Result<OriginalItem, ?> orig ) {
        return builder( orig, orig.getInput() );
    }

    public static <OriginalItem, Output> Builder<OriginalItem, Output> builder( final Result<?, ?> orig, final OriginalItem input ) {
        return new Builder<OriginalItem, Output>()
                .withInput( input )
                .withThrowables( orig.getThrowables() )
                .withWarningMessages( orig.getWarningMessages() )
                .withFailureMessages( orig.getFailureMessages() );
    }

    public OriginalItem getInput() {
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
        final List<String> results = new ArrayList<>();
        getWarningMessages().forEach( results::add );
        getFailureMessages().forEach( results::add );

        return Collections.unmodifiableList( results );
    }

    public static <OriginalItem, Output> Result<OriginalItem, Output> success( final OriginalItem originalItem, final Output output ) {
        return success( originalItem, output, Collections.emptyList() );
    }

    public static <OriginalItem, Output> Result<OriginalItem, Output> success( final OriginalItem originalItem, final Output output, final Iterable<String> warnings ) {
        return new Result<>( originalItem, output, false, warnings, Collections.emptyList(), null );
    }

    public static <OriginalItem, Output> Result<OriginalItem, Output> failed( final OriginalItem originalItem, final Throwable t ) {
        return failed( originalItem, Collections.emptyList(), t );
    }

    public static <OriginalItem, Output> Result<OriginalItem, Output> failed( final OriginalItem originalItem, final String failureMessage, final Throwable t ) {
        return failed( originalItem, failureMessage == null
            ? Collections.emptyList()
            : Collections.singletonList( failureMessage ), t );
    }

    public static <OriginalItem, Output> Result<OriginalItem, Output> failed( final OriginalItem originalItem, final String failureMessage ) {
        return failed( originalItem, failureMessage == null
            ? Collections.emptyList()
            : Collections.singletonList( failureMessage ), null );
    }

    public static <OriginalItem, Output> Result<OriginalItem, Output> failed( final OriginalItem originalItem, final Iterable<String> failureMessages, final Throwable t ) {
        if ( t != null ) {
            final String msg = originalItem + " - " + ( failureMessages == null
                ? ""
                : String.join( " | ", failureMessages ) );
            LOG.error( msg, t );
        }
        return new Result<>( originalItem, null, true, Collections.emptyList(), failureMessages, t == null
                ? Collections.emptyList()
                : Collections.singletonList( t ) );
    }

    public static <OriginalItem, Output> Result<OriginalItem, Output> failed( final OriginalItem originalItem, final Iterable<String> failureMessages ) {
        return failed( originalItem, failureMessages, null );
    }

    @Override
    public String toString() {
        if ( this.failed ) {
            return "FAIL: " + input + " [" + failureMessages.size() + " messages]";
        }
        return "SUCCESS: " + input + " => " + output + " [" + warningMessages.size() + " messages]";
    }


    /**
     * Create a new result by transforming the output value for success, using null for failure.
     * 
     * If this result is failed, the new result will be failed as well. If the success mapping function throws an exception,
     * the new result will be failed, too.
     * 
     * @param successMapper the mapping function
     * @return the new result.
     */
    public <NewOutput> Result<OriginalItem, NewOutput> map( @Nonnull final Function<Output, NewOutput> successMapper ) {
        return map( successMapper, o -> null); 
    }

        
    /**
     * Create a new result by transforming the output value for both success and failure cases. 
     * 
     * If this result is failed, the new result will be failed as well. If the success mapping function throws an exception,
     * the new result will be failed, too.
     * 
     * @param successMapper the mapping function
     * @param failureMapper the mapping function
     * @return the new result.
     */
    public <NewOutput> Result<OriginalItem, NewOutput> map( 
            @Nonnull final Function<Output, NewOutput> successMapper, 
            @Nonnull final Function<Output, NewOutput> failureMapper 
    ) {
        final Builder<OriginalItem, NewOutput> builder = Result.builder( this );
        
        if (this.isSuccess()) {
            try {
                return builder.withOutput( successMapper.apply( output ) ).success();
            } catch (final Throwable t) {
                return builder.failed( t );
            }
        }
        // originally failed
        try {
            return builder.withOutput( failureMapper.apply( output ) ).failed();
        } catch (final Throwable t) {
            return builder.failed( t );
        }
    }

}