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
package com.freiheit.fuava.simplebatch;

import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author tim.lessner@freiheit.com
 */
@Test
public class TestComposition {

    private static class FileContentPair {
        public final File file;
        public final String content;

        public FileContentPair( final File file, final String content ) {
            this.file = file;
            this.content = content;
        }
    }

    private final File exF1 = new File( "/tmp/exF1" );
    private final File exF2 = new File( "/tmp/exF2" );
    private final File exF3 = new File( "/tmp/exF3" );

    private final String f1String = "exF1";
    private final String f2String = "exF2";
    private final String f3String = "exF3";

    final Map<String, FileContentPair> testFileContentPairs = CollectionUtils.asMap(
            f1String, new FileContentPair( exF1, f1String ),
            f2String, new FileContentPair( exF2, f2String ),
            f3String, new FileContentPair( exF3, f3String ) );

    final List<Result<File, File>> nonExistingFiles = Collections.singletonList(
            asResult( new File( "/tmp/a/a" ) ) );

    @BeforeClass
    public void makeTestData() throws IOException {
        for ( final FileContentPair fileContentPair : testFileContentPairs.values() ) {
            try ( Writer fos = new OutputStreamWriter( new FileOutputStream( fileContentPair.file ), StandardCharsets.UTF_8 ) ) {
                fos.write( fileContentPair.content );
            }
        }
    }

    private <T> Result<T, T> asResult( final T data ) {
        return Result.success( data, data );
    }

    @Test
    public void testComposition() {
        final Processor<File, File, String> compose = makeComposedProcessorFileStringProcessor();
        final List<Result<File, File>> testFiles = testFileContentPairs
                .values()
                .stream()
                .map( fileContentPair -> fileContentPair.file )
                .map( file -> Result.success( file, file ) )
                .collect( Collectors.toList() );
        final Iterable<Result<File, String>> processed = compose.process( testFiles );

        for ( final Result r : processed ) {
            Assert.assertFalse( r.isFailed(), "Failed result did not fail!" );
            final FileContentPair fileContentPair = testFileContentPairs.get( r.getOutput() );
            Assert.assertNotNull( fileContentPair );
        }

    }

    @Test
    public void testForFailures() {
        final Processor<File, File, String> compose = makeComposedProcessorFileStringProcessor();

        final Iterable<Result<File, String>> processed = compose.process( nonExistingFiles );
        Assert.assertEquals( Collections.singletonList( processed ).size(), 1 );

        for ( final Result r : processed ) {
            Assert.assertTrue( r.isFailed(), "Failed result did not fail!" );
        }
    }

    private Processor<File, File, String> makeComposedProcessorFileStringProcessor() {
        return Processors.<File> fileMover( "/tmp" ).then( makeReadFilesToStringTestProcessor() );
    }

    private Processor<File, File, String> makeReadFilesToStringTestProcessor() {
        return Processors.singleItemFunction( File::getName );
    }
}
