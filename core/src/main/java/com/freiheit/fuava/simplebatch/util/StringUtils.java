package com.freiheit.fuava.simplebatch.util;


/**
 * Utility functions for string handling within the fuava simplebatch project.
 * 
 * @author klas
 *
 */
public final class StringUtils {

    private StringUtils() {
        // static util class constructor
    }

    /**
     * Converts the value to a string, but only the first maxLenght characters.
     * Append '...' if there were more characters in the original string.
     */
    public static String toMaxLengthString( final Object value, final int maxLength ) {
        if ( value == null ) {
            return "null";
        }
        final String str = value.toString();

        final int length = str.length();

        return str.substring( 0, Math.min( 40, length ) ) + ( length > maxLength
            ? "..."
            : "" );
    }

}
