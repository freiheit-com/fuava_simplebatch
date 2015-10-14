/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 09.10.15 by thomas.ostendorf@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.sftp;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

/**
 *
 * Fetches the data from a given filename that includes a timestamp.
 * The latest available timestamp is resolved to get the latest data. The rest of the data is moved
 * to a skipped directory.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpOldFilesMovingLatestFileFetcher implements Fetcher<SftpFilename> {

    private static final Logger LOG = LoggerFactory.getLogger( SftpOldFilesMovingLatestFileFetcher.class );

    private final RemoteClient remoteClient;

    private final String skippedFolder;
    private final String processingFolder;
    private final String incomingFilesFolder;
    private final FileType fileType;

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
     * @param fileType
     *            The type of file to be downloaded.
     */
    public SftpOldFilesMovingLatestFileFetcher(
            final RemoteClient remoteClient,
            final String skippedFolder,
            final String processingFolder,
            final String incomingFilesFolder, final FileType fileType ) {
        this.remoteClient = remoteClient;
        this.skippedFolder = skippedFolder;
        this.processingFolder = processingFolder;
        this.incomingFilesFolder = incomingFilesFolder;
        this.fileType = fileType;
    }

    /**
     * Fetches all files in location directory. Extracts the latest files that need to be downloaded.
     * All other files are moved a skipped folder on the sftp server.
     *
     * @return List of files for downloading.
     */
    @Override
    public Iterable<Result<FetchedItem<SftpFilename>, SftpFilename>> fetchAll() {
        try {
            final List<String> entryFileNameList = remoteClient.listFolder( incomingFilesFolder );

            //process only the current file type
            final List<String> filteredFileNamesList =
                    FilenameUtil.getAllMatchingFilenames( "", fileType, entryFileNameList, RemoteFileStatus.OK );
            return moveOldFilesToSkippedAndReturnLatestFilename( filteredFileNamesList, fileType );
        } catch ( final Throwable e ) {
            LOG.error( "Failed to acquire file list from remote server!" );
            final FetchedItem<SftpFilename> fetchedItem =
                    FetchedItem.of( new SftpFilename( incomingFilesFolder, "", null, "no timestamp" ), 1 );
            return Collections.singletonList( Result.<FetchedItem<SftpFilename>, SftpFilename>failed( fetchedItem, e ) );
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

        final ImmutableList.Builder<Result<FetchedItem<SftpFilename>, SftpFilename>> files = new ImmutableList.Builder<>();

        /*
         * We only extract the filename from the .ok-files in Order to find only
         * files that are ready.
         */
        final String latestDateExtracted = FilenameUtil.extractLatestDateFromFilenames(
                fileNamesList, fileType, RemoteFileStatus.OK );

        if ( latestDateExtracted == null ) {
            LOG.info( "No .ok file matching the schema found on the server. Nothing to download." );
            return files.build(); // return an empty list
        }

        // get list of all .ok files fulfilling the pattern defined by file type
        // make sure newer files are processed first
        final List<String> okFiles = Ordering.natural().reverse().immutableSortedCopy(
                FilenameUtil.getAllMatchingFilenames(
                        "", fileType, fileNamesList, RemoteFileStatus.OK ) );

        // extract the latest timestamp of all ok.-files.
        final long latestTimestamp = FilenameUtil.timestampToLong( latestDateExtracted );

        // move all skipped files to skipped folder, add all files for download to the result list
        for ( final String okFile : okFiles ) {
            if ( okFile != null ) {

                final long timestamp = FilenameUtil.getDateFromFilename( okFile );
                if ( isLatestFile( timestamp, latestTimestamp ) ) {
                    files.add( moveToProcessing( latestTimestamp, okFile ) );
                } else {
                    // this file is older then the latest one, move it to the skipped folder
                    silentlyMoveToSkipped( okFile );
                }
            }
        }

        final ImmutableList<Result<FetchedItem<SftpFilename>, SftpFilename>> result = files.build();
        // warn if too many or too few items returned
        if ( result.size() != 1 ) {
            LOG.warn( "Unexpected number of Items in result: " + result );
        }
        return result;
    }

    private Result<FetchedItem<SftpFilename>, SftpFilename> moveToProcessing( final long latestTimestamp, final String okFile ) {
        try {
            // Get file name of latest file for creating the SftpFilename
            final String dataFilenameOfLatestFile = FilenameUtil.getDataFileOfOkFile( this.fileType, okFile );

            remoteClient.createFolderIfNotExist( processingFolder );

            // Get full path and name for latest file
            final String fullPathAndNameOfLatestFile =
                    remoteClient.moveFileAndControlFileFromOneDirectoryToAnother( okFile, this.fileType,
                            incomingFilesFolder,
                            processingFolder );

            // Create the sftp file name.
            final SftpFilename sftpFilenameOfLatestFile =
                    new SftpFilename( dataFilenameOfLatestFile, fullPathAndNameOfLatestFile, this.fileType, Long.toString( latestTimestamp ) );
            final FetchedItem<SftpFilename> fetchedItem = FetchedItem.of( sftpFilenameOfLatestFile, 1 );

            return Result.success( fetchedItem, sftpFilenameOfLatestFile );

        } catch ( final Exception e ) {
            //HINT: failure may be logged in case another downloader instance just "stole" the file
            final FetchedItem<SftpFilename> fetchedItem =
                    FetchedItem.of( new SftpFilename( incomingFilesFolder, "", this.fileType, "no timestamp" ), 1 );

            return Result.<FetchedItem<SftpFilename>, SftpFilename> failed( fetchedItem, e );
        }
    }

    private void silentlyMoveToSkipped( final String okFile ) {
        try {
            // move files from location folder to skipped folder.
            remoteClient.createFolderIfNotExist( skippedFolder );
            remoteClient.moveFileAndControlFileFromOneDirectoryToAnother( okFile, this.fileType, incomingFilesFolder,
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
            if ( !Strings.isNullOrEmpty( filename ) ) {
                return filename;
            }
        }
        return null;
    }




}
