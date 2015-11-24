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
package com.freiheit.fuava.sftp.testclient;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.freiheit.fuava.sftp.RemoteClient;
import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.freiheit.fuava.simplebatch.util.FileUtils;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.jcraft.jsch.SftpException;

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
        final ImmutableMap.Builder<String, TestFolder<T>> b = ImmutableMap.builder();
        for (final Map.Entry<String, TestFolder<T>> e : folders.entrySet()) {
            b.put( e.getKey(), new TestFolder<>( ImmutableMap.copyOf(e.getValue().folderContent )) );
        }
        return b.build();
    }
    
    @Override
    public void moveFileOnRemoteSystem( final String sourcePath, final String destinationPath ) throws Exception {
        final TestFolder<T> sourceFolder =
                Preconditions.checkNotNull( getFolderOfFile( sourcePath ), "folder for source not found" );
        final TestFolder<T> targetFolder =
                Preconditions.checkNotNull( getFolderOfFile( destinationPath ), "folder for target not found" );
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
    public List<String> listFolder( final String pathToFiles ) throws Exception {
        final TestFolder<T> testFolder = this.folders.get( asFoldersKey( pathToFiles ) );
        final List<String> content = testFolder == null
            ? ImmutableList.<String> of()
            : FluentIterable.from( testFolder.getItemKeys() ).toSortedList( Ordering.natural() );
        return content;

    }

    @Override
    public InputStream downloadRemoteFile( final String pathToFile ) throws Exception {
        final String fileName = getFileName( pathToFile );
        final TestFolder<T> tf = getFolderOfFile( pathToFile );
        final T item = tf == null
            ? null
            : tf.getItem( fileName );
        return item == null
            ? null
            : inputStreamProvider.apply( item );
    }

    private TestFolder<T> getFolderOfFile( final String pathToFile ) {
        final String dirPath = getDirName( pathToFile );
        return this.folders.get( asFoldersKey( dirPath ) );
    }

    private String getDirName( final String pathToFile ) {
        final String parent = new File( pathToFile ).getParent() +"/";
        return parent;
    }

    private String getFileName( final String pathToFile ) {
        final String name = new File( pathToFile ).getName();
        return name;
    }

    @Override
    public void deleteFile( final String pathOfFileToDelete ) throws Exception {
        final TestFolder<T> tf = getFolderOfFile( pathOfFileToDelete );
        if ( tf != null ) {
            tf.removeItem( getFileName( pathOfFileToDelete ) );
        }
    }

    @Override
    public void createFolderIfNotExist( final String folderNameToCreate ) throws Exception {
        final String dirPath = folderNameToCreate;
        final String key = asFoldersKey( dirPath );
        if ( !this.folders.containsKey( key ) ) {
            this.folders.put( key, new TestFolder<>( ImmutableMap.<String, T> builder().build() ) );
        }
    }

    private String asFoldersKey( final String dirPath ) {
        return FileUtils.ensureTrailingSlash( dirPath );
    }
}