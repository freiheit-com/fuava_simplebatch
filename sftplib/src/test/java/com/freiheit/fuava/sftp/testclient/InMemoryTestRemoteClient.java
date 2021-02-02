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
package com.freiheit.fuava.sftp.testclient;

import com.freiheit.fuava.sftp.RemoteClient;
import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.freiheit.fuava.simplebatch.util.FileUtils;
import com.jcraft.jsch.SftpException;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class InMemoryTestRemoteClient<T> implements RemoteClient {

    private final ConcurrentMap<String, TestFolder<T>> folders;
    private final Function<T, InputStream> inputStreamProvider;

    public InMemoryTestRemoteClient( final Map<String, TestFolder<T>> initialState,
            final Function<T, InputStream> inputStreamProvider ) {
        this.folders = buildFoldersMap( initialState );
        this.inputStreamProvider = inputStreamProvider;
    }

    private ConcurrentMap<String, TestFolder<T>> buildFoldersMap( final Map<String, TestFolder<T>> initialState ) {
        final ConcurrentMap<String, TestFolder<T>> b = new ConcurrentHashMap<>();
        for ( final Map.Entry<String, TestFolder<T>> e : initialState.entrySet() ) {
            b.put( asFoldersKey( e.getKey() ), e.getValue() );
        }
        return b;
    }

    public Map<String, TestFolder<T>> getStateCopy() {
        return Collections.unmodifiableMap(
                folders.entrySet().stream()
                        .collect( Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new TestFolder<>( new LinkedHashMap<>( entry.getValue().folderContent ) )
                        ) ) );
    }
    
    @Override
    public void moveFileOnRemoteSystem( final String sourcePath, final String destinationPath ) {
        final TestFolder<T> sourceFolder =
                Objects.requireNonNull( getFolderOfFile( sourcePath ), "folder for source not found" );
        final TestFolder<T> targetFolder =
                Objects.requireNonNull( getFolderOfFile( destinationPath ), "folder for target not found" );
        final String sourceFileName = getFileName( sourcePath );
        final T item = sourceFolder.getItem( sourceFileName );
        targetFolder.addItem( getFileName( destinationPath ), item );
        sourceFolder.removeItem( sourceFileName );
    }

    @Override
    public String moveFileAndControlFileFromOneDirectoryToAnother( final String okFile, final FileType fileType,
            final String fromFolder,
            final String toFolder ) throws Exception {
        // this file is older then the latest one, move it to the skipped folder
        final String dataFilename = FilenameUtil.getDataFileOfOkFile( fileType, okFile );
        if ( dataFilename == null ) {
            throw new SftpException( 1, "Failed to locate data file for .ok file: " + okFile );
        }

        final String origOkFile = FileUtils.ensureTrailingSlash( fromFolder) + okFile;
        final String destOkFile = FileUtils.ensureTrailingSlash( toFolder ) + okFile;
        final String origDataFile = FileUtils.ensureTrailingSlash( fromFolder ) + dataFilename;
        final String destDataFile = FileUtils.ensureTrailingSlash( toFolder ) + dataFilename;

        // first move the data file
        moveFileOnRemoteSystem( origDataFile, destDataFile );
        //LOG.info( "Moved file " + origDataFile + " to folder " + destDataFile );

        // then move .ok file
        moveFileOnRemoteSystem( origOkFile, destOkFile );
        //LOG.info( "Moved .ok file " + origOkFile );

        return destDataFile;
    }

    @Override
    @Nonnull
    public List<String> listFolder( final String pathToFiles ) {
        final TestFolder<T> testFolder = this.folders.get( asFoldersKey( pathToFiles ) );
        return testFolder == null
            ? Collections.emptyList()
            : Collections.unmodifiableList( testFolder.getItemKeys()
                .stream()
                .sorted( Comparator.naturalOrder() )
                .collect( Collectors.toList() ) );

    }

    @Override
    public InputStream downloadRemoteFile( final String pathToFile ) {
        final String fileName = getFileName( pathToFile );
        final TestFolder<T> tf = getFolderOfFile( pathToFile );
        final T item = tf == null
            ? null
            : tf.getItem( fileName );
        final InputStream r = item == null
            ? null
            : inputStreamProvider.apply( item );
        if ( r == null ) {
            throw new IllegalArgumentException( "Unknown File " + pathToFile );
        }
        return r;
    }

    private TestFolder<T> getFolderOfFile( final String pathToFile ) {
        final String dirPath = getDirName( pathToFile );
        return this.folders.get( asFoldersKey( dirPath ) );
    }

    private String getDirName( final String pathToFile ) {
        return new File( pathToFile ).getParent() +"/";
    }

    private String getFileName( final String pathToFile ) {
        return new File( pathToFile ).getName();
    }

    @Override
    public void deleteFile( final String pathOfFileToDelete ) {
        final TestFolder<T> tf = getFolderOfFile( pathOfFileToDelete );
        if ( tf != null ) {
            tf.removeItem( getFileName( pathOfFileToDelete ) );
        }
    }

    @Override
    public void createFolderIfNotExist( final String folderNameToCreate ) {
        final String key = asFoldersKey( folderNameToCreate );
        if ( !this.folders.containsKey( key ) ) {
            this.folders.put( key, new TestFolder<>( Collections.emptyMap() ) );
        }
    }

    private String asFoldersKey( final String dirPath ) {
        return FileUtils.ensureTrailingSlash( dirPath );
    }
}