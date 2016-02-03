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

import javax.annotation.Nullable;

import com.freiheit.fuava.simplebatch.util.StringUtils;

/**
 * A FetchedItem is a container for a single item.
 * 
 * It is used to transport information about the original input value, the row
 * number wrt the complete input, and a possible identifying string of the item
 * being processed.
 * 
 * @author klas.kalass@freiheit.com
 *
 * @param <OriginalInput>
 */
public class FetchedItem<OriginalInput> {
    public static final int FIRST_ROW = 0;
    private final int num;
    private final OriginalInput value;
    @Nullable
    private final String rowIdentifier;

    protected FetchedItem( final OriginalInput value, final int num, @Nullable final String rowIdentifier ) {
        this.value = value;
        this.num = num;
        this.rowIdentifier = rowIdentifier;
    }

    /**
     * Creates a fetched Item from a value and row num. You should use the
     * creation function with a row identifier instead.
     */
    @Deprecated
    public static <OriginalInput> FetchedItem<OriginalInput> of( final OriginalInput value, final int rowNum ) {
        return of( value, rowNum, null );
    }

    /**
     * Creates a fetched Item from a value, row num and a row identifier.
     */
    public static <OriginalInput> FetchedItem<OriginalInput> of( final OriginalInput value, final int rowNum, @Nullable final String rowIdentifier ) {
        return new FetchedItem<OriginalInput>( value, rowNum, rowIdentifier );
    }
    
    /**
     * Creates a new FetchedItem from this one, associating a different Value with it
     * @param <OriginalInput2> The type of the value for the new FetchedItem instance.
     */
    public <OriginalInput2> FetchedItem<OriginalInput2> withValue( final OriginalInput2 value ) {
        return new FetchedItem<OriginalInput2>( value, this.num, this.rowIdentifier );
    }

    /**
     * The number of the item within the fetcher run.
     */
    public int getNum() {
        return num;
    }

    public OriginalInput getValue() {
        return value;
    }

    public String getIdentifier() {
        return rowIdentifier;
    }

    @Override
    public String toString() {

        if ( rowIdentifier == null ) {
            return "[Row: " + this.num + "] " + StringUtils.toMaxLengthString( value, 100 );
        }
        return "[" + rowIdentifier + " | Row: " + this.num + "] " + StringUtils.toMaxLengthString( value, 100 );
    }
}
