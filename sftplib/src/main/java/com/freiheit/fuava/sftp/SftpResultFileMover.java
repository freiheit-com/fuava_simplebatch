package com.freiheit.fuava.sftp;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.AbstractSingleItemProcessor;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor that moves the processed file to the archived folder.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpResultFileMover extends
        AbstractSingleItemProcessor<FetchedItem<SftpFilename>, ControlFilePersistenceOutputInfo, ControlFilePersistenceOutputInfo> {

    private static final Logger LOG = LoggerFactory.getLogger( SftpResultFileMover.class );

    private final SftpClient client;
    private final String archiveFolder;

    /**
     *
     *
     * @param client
     *            SFTP client.
     * @param archiveFolder
     *            Processed (downloaded) files are moved to this folder on
     *            remote server.
     */
    public SftpResultFileMover( final SftpClient client, final String archiveFolder ) {
        this.client = client;
        this.archiveFolder = archiveFolder;
    }

    public Result<FetchedItem<SftpFilename>, ControlFilePersistenceOutputInfo> processItem( final Result<FetchedItem<SftpFilename>, ControlFilePersistenceOutputInfo> data ) {
        if ( data.isFailed() ) {
            return Result.<FetchedItem<SftpFilename>, ControlFilePersistenceOutputInfo> builder( data ).failed();
        } else {
            try {

                // Sftp file name
                final SftpFilename sftpFilename = data.getInput().getValue();
                final String dataFilename = sftpFilename.getFilename();
                final String okFilename = FilenameUtil.getOkFileForDataFile( sftpFilename.getFileType(), dataFilename );

                final String dataFile = sftpFilename.getRemoteFullPath();
                final String okFile = FilenameUtil.getOkFileForDataFile( sftpFilename.getFileType(), dataFile );

                final String archivedDataFile = archiveFolder + dataFilename;
                final String archivedOkFile = archiveFolder + okFilename;

                // move the file to archive
                client.moveFile( dataFile, archivedDataFile );
                client.moveFile( okFile, archivedOkFile );
                LOG.info( "Moved downloaded file " + dataFilename + " to the archive folder on remote server" );

                return Result.success( data.getInput(), data.getOutput() );
            } catch ( final Throwable e ) {
                return Result.failed( data.getInput(), e );
            }
        }
    }




}
