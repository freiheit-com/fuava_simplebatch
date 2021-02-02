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
package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.nio.file.Path;

/**
 * @author tim.lessner@freiheit.com
 */
public class FailedToMoveFileException extends Exception {

    public FailedToMoveFileException( final String msg ) {
        super( msg );
    }

    public FailedToMoveFileException( final Path from, final Path to, final Throwable cause ) {
        super( "Cannot move " + from + " -> " + to + " due to " + cause.getMessage(), cause );
    }
}
