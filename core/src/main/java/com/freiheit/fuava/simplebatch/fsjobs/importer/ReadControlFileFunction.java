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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

/**
 * @author tim.lessner@freiheit.com
 */
public class ReadControlFileFunction implements Function<Path, ControlFile> {

    private static final Logger LOG = LoggerFactory.getLogger( ReadControlFileFunction.class );
    
    private final List<Path> baseDirs;

    public ReadControlFileFunction( final Path baseDir, final Path... alternativeBaseDirs ) {
        final List<Path> paths = new ArrayList<>( alternativeBaseDirs.length + 1 );
        paths.add( baseDir );
        Collections.addAll( paths, alternativeBaseDirs );
        this.baseDirs = Collections.unmodifiableList( paths );
    }

    private Path getBaseDir( final Path path ) {
        for ( final Path p: baseDirs ) {
            if ( path.startsWith( p ) ) {
                return p;
            }
        }
        throw new IllegalStateException( "Not in one of the known base dirs: " + path + ". Known dirs: " + baseDirs );
    }
    
    private ControlFile parseNewFormat( final Path path, final BufferedReader br ) throws IOException {
        final Properties prop = new Properties();
        prop.load( br );
        final String controlledFileName = prop.getProperty( "file" );
        final String originalFileName = prop.getProperty( "originalFileName" );
        final String logFileName = Objects.requireNonNull( prop.getProperty( "log" ), "Log file property is mandatory" );
        final String status = prop.getProperty( "status" );
        
        final Path baseDir = getBaseDir( path );
        return new ControlFile( 
                baseDir,
                controlledFileName == null || controlledFileName.isEmpty() ? Paths.get( "" ) : Paths.get( controlledFileName ) ,
                Paths.get( logFileName ), 
                baseDir.relativize( path ), 
                originalFileName,
                status
            );
    }

    @Override
    public ControlFile apply( final Path path ) {
        try {
            final File file = path.toFile();
            final Reader in = new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 );
            try ( BufferedReader br = new BufferedReader( in ) ) {
                final String firstLine = br.readLine();
                if ( firstLine == null || firstLine.isEmpty() ) {
                    LOG.info( "The Control-File " + file + " has no content" );
                    final Path baseDir = getBaseDir( path );
                    final Path relPathCtlFile = baseDir.relativize( path );
                    final String ctlFileName = relPathCtlFile.getFileName().toString();
                    final String fileName = getFileNameFromCtlFileName( ctlFileName );
                    final Path controlledFileName = relPathCtlFile.resolveSibling( fileName );
                    final Path logFileName = Paths.get( controlledFileName + ".log" );
                    return new ControlFile( baseDir, controlledFileName, logFileName, relPathCtlFile );

                } else if ( firstLine.startsWith( "#!VERSION=1" ) ) {
                    return parseNewFormat( path, br );
                } else {
                    final Path controlledFileName = Paths.get( firstLine );
                    final Path logFileName = Paths.get( controlledFileName + ".log" );
                    final Path baseDir = getBaseDir( path );
                    return new ControlFile( baseDir, controlledFileName, logFileName, baseDir.relativize( path ) );
                }
            }
        } catch ( final IOException e ) {
            throw new RuntimeException( e );
        }
    }

    private String getFileNameFromCtlFileName( final String ctlFileName ) {
        final int idx = ctlFileName.lastIndexOf( "." );
        if (idx < 0) {
            return ctlFileName +".notfound";
        }
        
        return ctlFileName.substring( 0, idx );
    }
}
