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
package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.result.Result;

public final class FailsafeIterator<OriginalInput> implements Iterator<Result<FetchedItem<OriginalInput>, OriginalInput>> {

    private final Iterator<OriginalInput> iterator;
    private Result<FetchedItem<OriginalInput>, OriginalInput> forceNextElement;
    private Boolean forceHasNext;
    private int num = FetchedItem.FIRST_ROW;

    public FailsafeIterator( final Iterator<OriginalInput> iterator ) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if ( forceHasNext != null ) {
            return forceHasNext.booleanValue();
        }
        try {
            return iterator.hasNext();
        } catch ( final Throwable t ) {
            forceHasNext = Boolean.TRUE;
            forceNextElement = Result.failed( nextFetchedItem( null ), "Failed to call hasNext on delegate iterator", t );
            return forceHasNext.booleanValue();
        }
    }

    private FetchedItem<OriginalInput> nextFetchedItem( final OriginalInput value ) {
        return FetchedItem.of( value, num++ );
    }

    @Override
    public Result<FetchedItem<OriginalInput>, OriginalInput> next() {
        if ( forceNextElement != null ) {
            final Result<FetchedItem<OriginalInput>, OriginalInput> r = forceNextElement;
            forceHasNext = Boolean.FALSE;
            return r;
        }
        try {
            final OriginalInput value = iterator.next();
            if ( value == null ) {
                return Result.failed( nextFetchedItem( null ), "Null is not a valid fetching result" );
            } else {
                return Result.success( nextFetchedItem( value ), value );
            }
        } catch ( final Throwable t ) {
            return Result.failed( nextFetchedItem( null ), "Failed to call next for delegate iterator", t );
        }
    }

}