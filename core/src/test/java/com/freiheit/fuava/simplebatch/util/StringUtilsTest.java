package com.freiheit.fuava.simplebatch.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Tests the {@link StringUtils}.
 */
public class StringUtilsTest {
    @Test( expectedExceptions = NullPointerException.class )
    public void testPadStartThrowsExceptionIfStringIsNull() {
        StringUtils.padStart( null, 5, '#');
    }

    @Test
    public void testPadStartReturnsInputIfInputIsLongerThanRequired() {
        assertEquals( StringUtils.padStart( "12345", 5, '#' ), "12345" );
        assertEquals( StringUtils.padStart( "1234567", 5, '#' ), "1234567" );
    }

    @Test
    public void testPadStartPrepandsCharacters() {
        assertEquals( StringUtils.padStart( "1234", 5, '#' ), "#1234" );
        assertEquals( StringUtils.padStart( "123", 5, '#' ), "##123" );
        assertEquals( StringUtils.padStart( "12", 5, '#' ), "###12" );
        assertEquals( StringUtils.padStart( "1", 5, '#' ), "####1" );
        assertEquals( StringUtils.padStart( "", 5, '#' ), "#####" );
    }
}
