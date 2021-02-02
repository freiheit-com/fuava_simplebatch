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
package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public interface FileWriterAdapter<Input, Output> extends FileOutputStreamAdapter<Input, Output> {
    @Override
    String getFileName( Result<Input, Output> result );

    @Override
    default public void writeToStream( final java.io.OutputStream outputStream, final Output data ) throws IOException {
        try ( OutputStreamWriter fos = new OutputStreamWriter( outputStream, StandardCharsets.UTF_8 ) ) {
            write( fos, data );
            fos.flush();
        }

    }

    void write( Writer writer, Output data ) throws IOException;
}