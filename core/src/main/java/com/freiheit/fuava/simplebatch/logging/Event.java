package com.freiheit.fuava.simplebatch.logging;

/**
 * Event that represents either fetching data, processing data, or perist data.
 * 
 * @author tim.lessner@freiheit.com
 */
public enum Event {
        FETCH,
        PROCESS,
        PROCESSING;
}
