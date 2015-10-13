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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.AbstractSingleItemProcessor;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.result.Result;

/**
 * Processor that moves the successdully processed file to the archived folder.
 *
 * Note that files with an error are not touched, but moved into a failed folder on the remote system.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpResultFileMover extends
        AbstractSingleItemProcessor<FetchedItem<SftpFilename>, ControlFilePersistenceOutputInfo, ControlFilePersistenceOutputInfo> {

    private static final Logger LOG = LoggerFactory.getLogger( SftpResultFileMover.class );

    private final RemoteClient client;
    private final String archiveFolder;

    /**
     * Constructs SftpResultFileMover.
     *
     * @param client
     *            SFTP client.
     * @param archiveFolder
     *            Processed (downloaded) files are moved to this folder on
     *            remote server.
     */
    public SftpResultFileMover( final RemoteClient client, final String archiveFolder ) {
        this.client = client;
        this.archiveFolder = archiveFolder;
    }

    /**
     *  Moves successfully processed file to archived folder.
     *
     * @param data that is moved on sftp.
     */
    @Override
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

                client.createFolderIfNotExist( archiveFolder );

                // move the file to archive
                client.moveFileOnRemoteSystem( dataFile, archivedDataFile );
                client.moveFileOnRemoteSystem( okFile, archivedOkFile );
                LOG.info( "Moved downloaded file " + dataFilename + " to the archive folder on remote server" );

                return Result.success( data.getInput(), data.getOutput() );
            } catch ( final Throwable e ) {
                return Result.failed( data.getInput(), e );
            }
        }
    }




}
