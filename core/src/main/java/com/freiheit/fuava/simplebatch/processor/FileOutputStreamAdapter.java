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
import java.io.OutputStream;
import java.nio.file.Path;

import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.Sysprops;

public interface FileOutputStreamAdapter<Input, Output> {
    String getFileName( Result<Input, Output> result );
    
    /**
     * Get the relative path to be used for persisting the given result.
     * @param result The result to persist
     * @return a path - relative to the download directory. This may be directly a filename, or a file in a subdir 
     */
    default Path getRelativeFilePath( final Result<Input, Output> result ) {
        return prependSubdirs( getFileName(result) );
    }

    default Path prependSubdirs( final String filename ) {
        return Sysprops.SUBDIR_STRATEGY.prependSubdir( filename );
    }

    void writeToStream( OutputStream outputStream, Output data ) throws IOException;
}