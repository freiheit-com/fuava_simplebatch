package com.freiheit.fuava.simplebatch.fetch;

public class FetchedItem<T> {
    private final int num;
    private final T value;

    protected FetchedItem(T value, int num) {
        this.value = value;
        this.num = num;
    }

    public static <T> FetchedItem<T> of(T value, int rowNum) {
        return new FetchedItem<T>(value, rowNum);

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
        return "[" + this.num + "] " + value;
    }
}
