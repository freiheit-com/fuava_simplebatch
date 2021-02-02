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

import java.util.Optional;
import java.util.stream.StreamSupport;

public final class ComposedResult<A, B> {
    public static <A, B> ComposedResult<A, B> of( final Result<A, ?> orig ) {
        return new ComposedResult<>( orig );
    }

    private final Result.Builder<A, B> builder;
    private final Object intermediateValue;

    private ComposedResult( final Result<A, ?> orig ) {
        builder = Result.<A, B> builder( orig );
        intermediateValue = orig.getOutput();
    }

    public Result<A, B> failed( final String message ) {
        return builder.withFailureMessage( message ).failed();
    }

    /**
     * If there are no results, fail. If there is one successful result, return
     * success. Add warnings for any values that exceed.
     */
    public Result<A, B> compose( final Iterable<? extends Result<?, B>> results ) {
        if ( results == null || IterableUtils.isEmpty( results ) ) {
            return builder.withFailureMessage( "No intermediate results found. Intermediate input was " + intermediateValue ).failed();
        }
        final Optional<? extends Result<?, B>> firstSuccess = StreamSupport.stream( results.spliterator(), false )
                .filter( Result::isSuccess )
                .findFirst();
        for ( final Result<?, B> r : results ) {
            // add everything that was accumulated in the composed results
            builder
                    .withFailureMessages( r.getFailureMessages() )
                    .withWarningMessages( r.getWarningMessages() )
                    .withThrowables( r.getThrowables() );
        }
        return firstSuccess.map( bResult -> builder.withOutput( bResult.getOutput() ).success() )
                .orElseGet( builder::failed );
    }
}