package com.freiheit.fuava.simplebatch.result;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class ResultTest {
    
    @DataProvider( name = "dataProviderSuccessMap" )
    public Object[][] dataProviderSuccessMap() {
        
        return new Object[][] {
            {Result.success( "1", "1" ), 1, true, false},
            {Result.failed( "1", /*failure message*/"1" ), null, false, false},
            {Result.success( "asfd", "asfd" ), null, false, true},
            {Result.success( "0", "0" ), 0, true, false},
            {Result.success( "-1320", "-1320" ), -1320, true, false},
        }; 
    }

    @Test( dataProvider = "dataProviderSuccessMap" )
    public void testSuccessMapping(final Result<String, String> initialResult, final Integer expectedOutput, final boolean expectedSuccess, final boolean expectNewFailure) {
        final List<String> initialMessages = initialResult.getAllMessages();
        final Result<String, Integer> result = initialResult.map( v -> Integer.parseInt(v) );
        Assert.assertEquals( result.isSuccess(), expectedSuccess );
        Assert.assertEquals( result.isFailed(), !expectedSuccess );
        Assert.assertEquals( result.getOutput(), expectedOutput );
        if (expectNewFailure) {
            Assert.assertEquals( Iterables.size( result.getFailureMessages() ), Iterables.size( initialMessages ) + 1 );
        } else {
            Assert.assertEquals( result.getFailureMessages(), initialMessages );
        }
    }
    
    @DataProvider( name = "dataProviderMap" )
    public Object[][] dataProviderMap() {
        
        return new Object[][] {
            {Result.success( "1", "1" ), "Mapping: Success Output 1"},
            {Result.failed( "1", /*failure message*/"1" ), "Mapping: Failed Output"}
        }; 
    }

    
    @Test( dataProvider = "dataProviderMap" )
    public void testMap(final Result<String, String> initialResult, final String expectedOutput) {

        final Result<String,String> map = initialResult.map( v -> "Mapping: Success Output " + v, v2 -> "Mapping: Failed Output" );
        Assert.assertEquals( map.getOutput(), expectedOutput );
    }
    
}

