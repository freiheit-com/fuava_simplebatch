package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.util.StringUtils;

public class FetchedItem<T> {
    public static final int FIRST_ROW = 0;
    private final int num;
    private final T value;

    protected FetchedItem( final T value, final int num ) {
        this.value = value;
        this.num = num;
    }

    public static <T> FetchedItem<T> of( final T value, final int rowNum ) {
        return new FetchedItem<T>( value, rowNum );

    }

    /**
     * The number of the item within the fetcher run.
     */
    public int getNum() {
        return num;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "[" + this.num + "] " + StringUtils.toMaxLengthString( value, 40 );
    }
}
