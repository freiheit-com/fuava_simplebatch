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
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.fsjobs.importer.FailedToMoveFileException;
import com.freiheit.fuava.simplebatch.util.Sysprops;

/**
 * @author tim.lessner@freiheit.com
 */
public class ControlFileMover {
    private static final CopyOption[] MOVE_OPTIONS = Sysprops.ATOMIC_MOVE ? new CopyOption[] { StandardCopyOption.ATOMIC_MOVE } : new CopyOption[0];
    
    public static void move( final ControlFile source, final ControlFile target ) throws FailedToMoveFileException {
        if ( source == null ) {
            throw new FailedToMoveFileException( "Cannot Move null control file." );
        }
        
        final Path controlFile = source.getControlFile();
        
        moveFile( controlFile, target.getControlFile() );
        
        final Path logFile =  source.getLogFile();
        if ( logFile != null && Files.exists( logFile ) ) {
            moveFile( logFile, target.getLogFile() );
        }
        
        final Path controlledFile = source.getControlledFile();
        if ( controlledFile != null && Files.exists( controlledFile ) ) {
            moveFile( controlledFile, target.getControlledFile() );
        }
    }

    private static Path moveFile( final Path source, final Path target ) throws FailedToMoveFileException {
        try {
            // Note that Atomic Move should be enforced, but currently (2016-01-14) there are users of simplebatch
            // for whom an enforcement of atomic moves would be a breaking change.
            final Path targetDir = target.getParent();
            Files.createDirectories( targetDir);
            
            return Files.move( source, target, MOVE_OPTIONS );
        } catch ( final IOException e ) {
            throw new FailedToMoveFileException( source, target, e );
        }
    }
}
