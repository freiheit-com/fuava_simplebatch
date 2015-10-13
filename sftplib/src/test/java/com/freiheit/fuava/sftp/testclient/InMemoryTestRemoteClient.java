package com.freiheit.fuava.sftp.testclient;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.freiheit.fuava.sftp.RemoteClient;
import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.jcraft.jsch.SftpException;

public final class InMemoryTestRemoteClient<T> implements RemoteClient {

    private final Map<String, TestFolder<T>> folders;
    private final Function<T, InputStream> inputStreamProvider;

    public InMemoryTestRemoteClient( final Map<String, TestFolder<T>> initialState,
            final Function<T, InputStream> inputStreamProvider ) {
        this.folders = initialState;
        this.inputStreamProvider = inputStreamProvider;
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

        final String origOkFile = fromFolder + okFile;
        final String destOkFile = toFolder + okFile;
        final String origDataFile = fromFolder + dataFilename;
        final String destDataFile = toFolder + dataFilename;

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
        final TestFolder<T> testFolder = this.folders.get( pathToFiles );
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
        return this.folders.get( dirPath );
    }

    private String getDirName( final String pathToFile ) {
        return new File( pathToFile ).getParent();
    }

    private String getFileName( final String pathToFile ) {
        return new File( pathToFile ).getName();
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
        if ( !this.folders.containsKey( dirPath ) ) {
            this.folders.put( dirPath, new TestFolder<>( new HashMap<>() ) );
        }
    }
}