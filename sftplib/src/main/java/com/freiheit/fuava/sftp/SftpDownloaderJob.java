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

import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;

import java.io.InputStream;

/**
 * Standard Sftp Downloader Job for the purpose of downloading and processing
 * the newest file in a given directory on a remote system.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpDownloaderJob {

    private SftpDownloaderJob() {
    }

    public static CtlDownloaderJob.Configuration createDownloadConfig( final String downloadingDir ) {
        return new CtlDownloaderJob.ConfigurationImpl().setDownloadDirPath(
                downloadingDir ).setControlFileEnding( ".ctl" );
    }

    // Make sure that an remote client is disconnected after job is done.
    public static BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> makeDownloaderJob(
            final CtlDownloaderJob.Configuration config,
            final RemoteClient client,
            final FileType fileType ) {
        final Processor<FetchedItem<SftpFilename>, InputStream, ControlFilePersistenceOutputInfo>
                localFilePersister = Processors.controlledFileWriter( config.getDownloadDirPath(), config.getControlFileEnding(),
                new ProgressLoggingFileWriterAdapter() );

        final SftpFileProcessor downloader =
                new SftpFileProcessor( client, client.getRemoteConfiguration().getArchivedFolder() );

        SftpResultFileMover remoteFileMover =
                new SftpResultFileMover( client, client.getRemoteConfiguration().getArchivedFolder() );
        return new BatchJob.Builder<SftpFilename, ControlFilePersistenceOutputInfo>()
                .setFetcher(
                        new SftpOldFilesMovingLatestFileFetcher(
                                client,
                                client.getRemoteConfiguration().getSkippedFolder(),
                                client.getRemoteConfiguration().getProcessingFolder(),
                                client.getRemoteConfiguration().getLocationFolder(),
                                fileType ) )
                .addListener( new BatchStatisticsLoggingListener<>( "BATCH" ) )
                .addListener( new ItemProgressLoggingListener<>( "ITEM" ) )
                .setProcessor( Processors.compose( Processors.compose( remoteFileMover, localFilePersister ), downloader ) )

                .build();

    }

}
