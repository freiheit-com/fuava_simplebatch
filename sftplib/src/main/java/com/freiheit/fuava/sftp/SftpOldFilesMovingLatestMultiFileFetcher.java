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
package com.freiheit.fuava.sftp;

import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.result.Result;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 * Fetches the data from a given filename that includes a timestamp.
 * The latest available timestamp is resolved to get the latest data. The rest of the data is moved
 * to a skipped directory.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public abstract class SftpOldFilesMovingLatestMultiFileFetcher implements Fetcher<SftpFilename> {
    private static final Logger LOG = LoggerFactory.getLogger( SftpOldFilesMovingLatestMultiFileFetcher.class );

    private final RemoteClient remoteClient;

    private final String skippedFolder;
    private final String processingFolder;
    private final String incomingFilesFolder;
    private final boolean moveToProcessing;

    /**
     * ctor.
     *
     * @param remoteClient
     *            SFTP client
     * @param incomingFilesFolder
     *            Where to locate files to move.
     * @param skippedFolder
     *            Full path to the folder for skipped files. Fetcher moves
     *            outdated files straight to the skipped folder.
     * @param processingFolder
     *            Full path to the folder which contains the files that are
     */
    public SftpOldFilesMovingLatestMultiFileFetcher(
            final RemoteClient remoteClient,
            final String skippedFolder,
            final String processingFolder,
            final String incomingFilesFolder ) {
        Objects.requireNonNull( remoteClient, "Remote client must be provided but was null" );
        Objects.requireNonNull( skippedFolder, "Skipped folder must be provided, but was null" );
        Objects.requireNonNull( incomingFilesFolder, "Incoming folder must be provided, but was null" );
        this.remoteClient = remoteClient;
        this.skippedFolder = skippedFolder;
        this.processingFolder = processingFolder;
        this.incomingFilesFolder = incomingFilesFolder;
        this.moveToProcessing = processingFolder != null;
    }

    /**
     * Constructor for a fetcher that does not move files to a processing-dir.
     *
     * @param remoteClient
     *            SFTP client
     * @param incomingFilesFolder
     *            Where to locate files to move.
     * @param skippedFolder
     *            Full path to the folder for skipped files. Fetcher moves
     *            outdated files straight to the skipped folder.
     */
    public SftpOldFilesMovingLatestMultiFileFetcher(
            final RemoteClient remoteClient,
            final String skippedFolder,
            final String incomingFilesFolder ) {
        this( remoteClient, skippedFolder, null, incomingFilesFolder );
    }

    /**
     * Group all files by FileType that shall be processed.
     */
    protected abstract Map<FileType, List<String>> byType( List<String> filenames );

    /**
     * Fetches all files in location directory. Extracts the latest files that need to be downloaded.
     * All other files are moved a skipped folder on the sftp server.
     *
     * @return List of files for downloading.
     */
    @Override
    public Iterable<Result<FetchedItem<SftpFilename>, SftpFilename>> fetchAll() {
        try {
            final List<String> all = remoteClient.listFolder( incomingFilesFolder );

            final Map<FileType, List<String>> types = byType( all );

            final List<Result<FetchedItem<SftpFilename>, SftpFilename>> r = new ArrayList<>();
            for ( final Map.Entry<FileType, List<String>> e : types.entrySet() ) {
                final FileType fileType = e.getKey();
                final List<String> entryFileNameList = e.getValue();
                //process only the current file type
                try {
                    final List<String> filteredFileNamesList =
                            FilenameUtil.getAllMatchingFilenames( "", fileType, entryFileNameList, RemoteFileStatus.OK );
                    moveOldFilesToSkippedAndReturnLatestFilename( filteredFileNamesList, fileType ).forEach( r::add );
                } catch ( final Throwable e2 ) {
                    LOG.error( "Failed to filter file list from remote server! Folder: "  + incomingFilesFolder + ", File Type: " + fileType + " - " + entryFileNameList, e2 );
                    final FetchedItem<SftpFilename> fetchedItem =
                            FetchedItem.of( new SftpFilename( incomingFilesFolder, "", null, "no timestamp" ), 1 );
                    r.add( Result.failed( fetchedItem, e2 ) );
                }
            }
            return Collections.unmodifiableList( r );
        } catch ( final Throwable e ) {
            LOG.error( "Failed to acquire file list from remote server! Folder: " + incomingFilesFolder, e );
            final FetchedItem<SftpFilename> fetchedItem =
                    FetchedItem.of( new SftpFilename( incomingFilesFolder, "", null, "no timestamp" ), 1 );
            return Collections.singletonList( Result.failed( fetchedItem, e ) );
        }
    }

    /**
     * Filters out the files that need to be downloaded.
     *
     * @param fileNamesList all files on the sftp server with a desired pattern in the file name.
     * @param fileType the file type that needs to be processed.
     * @return a list of files that need to be downloaded.
     */
    protected Iterable<Result<FetchedItem<SftpFilename>, SftpFilename>> moveOldFilesToSkippedAndReturnLatestFilename(
            final List<String> fileNamesList,
            final FileType fileType ) throws SftpException, JSchException, ParseException, FileNotFoundException {
        /*
         * We only extract the filename from the .ok-files in Order to find only
         * files that are ready.
         */
        final String latestDateExtracted = FilenameUtil.extractLatestDateFromFilenames(
                fileNamesList, fileType, RemoteFileStatus.OK );

        if ( latestDateExtracted == null ) {
            LOG.info( "No .ok file matching the schema found on the server. Nothing to download." );
            return Collections.emptyList(); // return an empty list
        }
        final List<Result<FetchedItem<SftpFilename>, SftpFilename>> files = new ArrayList<>( fileNamesList.size() );

        // get list of all .ok files fulfilling the pattern defined by file type
        // make sure newer files are processed first
        final List<String> okFiles = FilenameUtil.getAllMatchingFilenames( "", fileType, fileNamesList, RemoteFileStatus.OK )
                .stream()
                .sorted( Comparator.<String>naturalOrder().reversed() )
                .collect( Collectors.toList() );

        // extract the latest timestamp of all ok.-files.
        final long latestTimestamp = FilenameUtil.timestampToLong( latestDateExtracted );

        // move all skipped files to skipped folder, add all files for download to the result list
        for ( final String okFile : okFiles ) {
            if ( okFile != null ) {
                final long timestamp = FilenameUtil.getDateFromFilename( okFile );
                if ( isLatestFile( timestamp, latestTimestamp ) ) {
                    if ( moveToProcessing ) {
                        files.add( moveToProcessing( fileType, latestTimestamp, okFile ) );
                    } else {
                        files.add( success( fileType, timestamp, okFile ) );
                    }
                } else {
                    // this file is older then the latest one, move it to the skipped folder
                    silentlyMoveToSkipped( fileType, okFile );
                }
            }
        }
        // warn if too many or too few items returned
        if ( files.size() != 1 ) {
            LOG.warn( "Unexpected number of Items in result: " + files );
        }
        return Collections.unmodifiableList( files );
    }

    private Result<FetchedItem<SftpFilename>, SftpFilename> success(
            final FileType fileType,
            final long timestamp,
            final String okFile ) {
        // Get file name of latest file for creating the SftpFilename
        final String dataFilename = FilenameUtil.getDataFileOfOkFile( fileType, okFile );
        final SftpFilename sftpFilename = new SftpFilename(
                dataFilename,
                incomingFilesFolder + File.separator + dataFilename,
                fileType,
                Long.toString( timestamp ) );
        final FetchedItem<SftpFilename> fetchedItem = FetchedItem.of( sftpFilename, 1 );
        return Result.success( fetchedItem, sftpFilename );
    }

    private Result<FetchedItem<SftpFilename>, SftpFilename> moveToProcessing( final FileType fileType, final long latestTimestamp,
            final String okFile ) {
        try {
            // Get file name of latest file for creating the SftpFilename
            final String dataFilenameOfLatestFile = FilenameUtil.getDataFileOfOkFile( fileType, okFile );

            remoteClient.createFolderIfNotExist( processingFolder );

            // Get full path and name for latest file
            final String fullPathAndNameOfLatestFile =
                    remoteClient.moveFileAndControlFileFromOneDirectoryToAnother( okFile, fileType,
                            incomingFilesFolder,
                            processingFolder );

            // Create the sftp file name.
            final SftpFilename sftpFilenameOfLatestFile =
                    new SftpFilename( dataFilenameOfLatestFile, fullPathAndNameOfLatestFile, fileType,
                            Long.toString( latestTimestamp ) );
            final FetchedItem<SftpFilename> fetchedItem = FetchedItem.of( sftpFilenameOfLatestFile, 1 );

            return Result.success( fetchedItem, sftpFilenameOfLatestFile );

        } catch ( final Exception e ) {
            //HINT: failure may be logged in case another downloader instance just "stole" the file
            final FetchedItem<SftpFilename> fetchedItem =
                    FetchedItem.of( new SftpFilename( incomingFilesFolder, "", fileType, "no timestamp" ), 1 );

            return Result.<FetchedItem<SftpFilename>, SftpFilename> failed( fetchedItem, e );
        }
    }

    private void silentlyMoveToSkipped( final FileType fileType, final String okFile ) {
        try {
            // move files from location folder to skipped folder.
            remoteClient.createFolderIfNotExist( skippedFolder );
            remoteClient.moveFileAndControlFileFromOneDirectoryToAnother( okFile, fileType, incomingFilesFolder,
                    skippedFolder );
        } catch ( final Exception e ) {
            // ignore this error, since this file might have been just processed by another downloader
            LOG.error( e.getMessage(), e );
        }
    }

    /**
     * Checks if current file is newer than the last one downloaded.
     *
     * @param currentFileTimestamp is the time stamps of the possibly new file to download.
     * @param latestSavedTimestamp is the timestamp of the last file downloaded.
     * @return true or false.
     */
    protected boolean isLatestFile( final long currentFileTimestamp, final long latestSavedTimestamp ) {
        return currentFileTimestamp >= latestSavedTimestamp;
    }


    /**
     * Returns a filename for an lsEntry.
     */
    @CheckForNull
    public static String lsEntryToFilename( final ChannelSftp.LsEntry lsEntry ) {
        if ( lsEntry != null ) {
            final String filename = lsEntry.getFilename();
            if ( filename != null && !filename.isEmpty() ) {
                return filename;
            }
        }
        return null;
    }




}
