package com.freiheit.fuava.sftp;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.AbstractSingleItemProcessor;
import com.freiheit.fuava.simplebatch.result.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Processor for the file one wnats to download.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpDownloadLatestFileDownloader extends
        AbstractSingleItemProcessor<FetchedItem<SftpFilename>, SftpFilename, InputStream> {

    private static final Logger LOG = LoggerFactory.getLogger( SftpDownloadLatestFileDownloader.class );

    private final SftpClient client;
    private final String archiveFolder;

    /**
     *
     * @param client
     *            SFTP client.
     * @param archiveFolder
     *            Processed (downloaded) files are moved to this folder on
     *            remote server.
     */
    public SftpDownloadLatestFileDownloader( final SftpClient client, final String archiveFolder ) {
        this.client = client;
        this.archiveFolder = archiveFolder;
    }

    public Result<FetchedItem<SftpFilename>, InputStream> processItem( final Result<FetchedItem<SftpFilename>, SftpFilename> data ) {
        if ( data.isFailed() ) {
            return Result.<FetchedItem<SftpFilename>, InputStream> builder( data ).failed();
        } else {
            try {
                final InputStream inputStream = client.getFile( data.getOutput().getRemoteFullPath() );
//                final byte[] fileData = ByteStreams.toByteArray( inputStream );

//                final String dataFilename = data.getOutput().getFilename();
//                final String okFilename = FilenameUtil.getOkFileForDataFile( data.getOutput().getFileType(), dataFilename );
//
//                final String dataFile = data.getOutput().getRemoteFullPath();
//                final String okFile = FilenameUtil.getOkFileForDataFile( data.getOutput().getFileType(), dataFile );
//
//                final String archivedDataFile = archiveFolder + dataFilename;
//                final String archivedOkFile = archiveFolder + okFilename;
//
//                // move the file to archive
//                client.moveFile( dataFile, archivedDataFile );
//                client.moveFile( okFile, archivedOkFile );
//                LOG.info( "Moved downloaded file " + dataFilename + " to the archive folder on remote server" );

                return Result.success( data.getInput(), inputStream );
            } catch ( final Throwable e ) {
                return Result.failed( data.getInput(), e );
            }
        }
    }




}
