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

    /**
     * Pads a given String with a given character until it reaches a specific targetlength.
     */
    public static String padStart( final String str, final int targetLength, final char filler ) {
        if ( str == null ) {
            throw new NullPointerException( "The input string may not be null!" );
        }
        if ( str.length() >= targetLength ) {
            return str;
        }
        final StringBuilder result = new StringBuilder();
        for ( int i = 0; i < targetLength - str.length(); i++ ) {
            result.append( filler );
        }
        return result.append( str ).toString();
    }

}
