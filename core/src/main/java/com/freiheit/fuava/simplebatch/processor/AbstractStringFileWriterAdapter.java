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
package com.freiheit.fuava.simplebatch.processor;

import java.io.IOException;
import java.io.Writer;

import com.freiheit.fuava.simplebatch.result.Result;

public class AbstractStringFileWriterAdapter<Input, Output> implements FileWriterAdapter<Input, Output> {

    @Override
    public String getFileName( final Result<Input, Output> result ) {
        final Input input = result.getInput();
        return input == null
            ? "null"
            : input.toString();
    }

    @Override
    public void write( final Writer writer, final Output data ) throws IOException {
        if ( writer != null ) {
            writer.write( data == null
                ? ""
                : data.toString() );
        }
    }

}
