package com.freiheit.fuava.sftp;

import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.sftp.util.ConvertUtil;
import com.freiheit.fuava.sftp.util.SftpFileType;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;

/**
 *
 * Fetches the data from a given filename that includes a timestamp.
 * The latest available timestamp is resolved to get the latest data. The rest
 * is skipped.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpDownloadLatestFileFetcher implements Fetcher<SftpFilename> {

    private static final Logger LOG = LoggerFactory.getLogger( SftpDownloadLatestFileFetcher.class );

    private final SftpClient sftpClient;

    private final String skippedFolder;
    private final String processingFolder;
    private final String filesLocationFolder;
    private final SftpFileType fileType;

    /**
     * ctor.
     *
     * @param sftpClient
     *            SFTP client
     * @param filesLocationFolder
     *            Where to locate files to move.
     * @param skippedFolder
     *            Full path to the folder for skipped files. Fetcher moves
     *            outdated files straight to the skipped folder.
     * @param processingFolder
     *            Full path to the folder which contains the files that are
     * @param fileType
     *            The type of file to be downloaded.
     */
    public SftpDownloadLatestFileFetcher( final SftpClient sftpClient, final String skippedFolder, final String processingFolder,
            final String filesLocationFolder, final SftpFileType fileType ) {
        this.sftpClient = sftpClient;
        this.skippedFolder = skippedFolder;
        this.processingFolder = processingFolder;
        this.filesLocationFolder = filesLocationFolder;
        this.fileType = fileType;
    }

    /**
     * Fetches all the latest files.
     *
     * @return List of files for downloading.
     */
    @Override
    public Iterable<Result<FetchedItem<SftpFilename>, SftpFilename>> fetchAll() {
        try {
            final List<ChannelSftp.LsEntry> entryList = sftpClient.listFolder( filesLocationFolder );
            final ImmutableList.Builder<Result<FetchedItem<SftpFilename>, SftpFilename>> processedFiles =
                    new ImmutableList.Builder();
            final List<String> fileNamesList = ConvertUtil.convertList( entryList, SftpDownloadLatestFileFetcher::lsEntryToFilename );

            //process only the current file type
            final List<String> filteredFileNamesList =
                    FilenameUtil.getAllMatchingFilenames( "", fileType, fileNamesList, RemoteFileStatus.OK );
            final Iterable<Result<FetchedItem<SftpFilename>, SftpFilename>> results =
                    processFiles( filteredFileNamesList, fileType );
            processedFiles.addAll( results );

            return processedFiles.build();
        } catch ( final Throwable e ) {
            LOG.error( "Failed to acquire file list from remote server!" );
            final FetchedItem<SftpFilename> fetchedItem =
                    FetchedItem.of( new SftpFilename( filesLocationFolder, "", null, "no timestamp" ), 1 );
            return Collections.singletonList( Result.<FetchedItem<SftpFilename>, SftpFilename>failed( fetchedItem, e ) );
        }
    }

    /**
     * Filters out the files that need to be downloaded.
     *
     * @param fileNamesList all files on the sftp server with a desired pattern in the file name.
     * @param sftpFileType the file type that needs to be processed.
     * @return a list of files that need to be downloaded.
     * @throws SftpException
     * @throws JSchException
     * @throws ParseException
     * @throws FileNotFoundException
     */
    protected Iterable<Result<FetchedItem<SftpFilename>, SftpFilename>> processFiles(
            final List<String> fileNamesList,
            final SftpFileType sftpFileType ) throws SftpException, JSchException, ParseException, FileNotFoundException {

        final ImmutableList.Builder<Result<FetchedItem<SftpFilename>, SftpFilename>> files = new ImmutableList.Builder<>();

        /*
         * We only extract the filename from the .ok-files in Order to find only
         * files that are ready.
         */
        final String latestDateExtracted = FilenameUtil.extractLatestDateFromFilenames(
                fileNamesList, sftpFileType, RemoteFileStatus.OK );

        if ( latestDateExtracted == null ) {
            LOG.info( "No .ok file matching the schema found on the server. Nothing to download." );
            return files.build(); // return an empty list
        }

        // get list of all .ok files fulfilling the pattern defined by file type
        // make sure newer files are processed first
        final List<String> okFiles = Ordering.natural().reverse().immutableSortedCopy(
                FilenameUtil.getAllMatchingFilenames(
                        "", sftpFileType, fileNamesList, RemoteFileStatus.OK ) );

        final long latestTimestamp = FilenameUtil.timestampToLong( latestDateExtracted );

        // move all skipped files to skipped folder, add all files for download to the result list
        for ( final String okFile : okFiles ) {
            if ( okFile != null ) {
                final Result<FetchedItem<SftpFilename>, SftpFilename> result = moveFiles( latestTimestamp, sftpFileType, okFile );
                if ( result != null ) {
                    files.add( result );
                }
            }
        }

        return files.build();
    }

    /**
     * Moves files on sftp server.
     *
     * @param latestTimestamp is used to decide whether a file is moved on the sftp server or not.
     * @param fileType identifies the file that needs to be processed.
     * @param okFile name is is used to extract the timestamp of the existing files on the sftp server.
     * @return
     */
    protected Result<FetchedItem<SftpFilename>, SftpFilename> moveFiles( final long latestTimestamp, final SftpFileType fileType,
            final String okFile ) {
        final long timestamp = FilenameUtil.getDateFromFilename( okFile );
        if ( timestamp < latestTimestamp ) {
            // this file is older then the latest one, move it to the skipped folder
            try {
                sftpClient.moveDataAndOkFile( okFile, fileType, filesLocationFolder, skippedFolder );
            } catch ( final SftpException e ) {
                // ignore this error, since this file might have been just processed by another downloader
                LOG.error( e.getMessage(), e );
            }

            return null;
        } else {

            // this file is older then the latest one, move it to the skipped folder
            try {
                final String dataFilename = FilenameUtil.getDataFileOfOkFile( fileType, okFile );
                final String fullPathAndName =
                        sftpClient.moveDataAndOkFile( okFile, fileType, filesLocationFolder, processingFolder );

                final SftpFilename sftpFilename =
                        new SftpFilename( dataFilename, fullPathAndName, fileType, Long.toString( latestTimestamp ) );
                final FetchedItem<SftpFilename> fetchedItem = FetchedItem.of( sftpFilename, 1 );

                return Result.success( fetchedItem, sftpFilename );

            } catch ( final SftpException e ) {
                //HINT: failure may be logged in case another downloader instance just "stole" the file
                final FetchedItem<SftpFilename> fetchedItem =
                        FetchedItem.of( new SftpFilename( filesLocationFolder, "", fileType, "no timestamp" ), 1 );
                return Result.<FetchedItem<SftpFilename>, SftpFilename> failed( fetchedItem, e );
            }

        }

    }


    /**
     * Returns a filename for an lsEntry.
     */
    @CheckForNull
    protected static String lsEntryToFilename( final ChannelSftp.LsEntry lsEntry ) {
        if ( lsEntry != null ) {
            final String filename = lsEntry.getFilename();
            if ( !Strings.isNullOrEmpty( filename ) ) {
                return filename;
            }
        }
        return null;
    }




}
