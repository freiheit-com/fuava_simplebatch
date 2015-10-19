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
import com.freiheit.fuava.sftp.util.RemoteConfiguration;
import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.util.FileUtils;

/**
 * Standard Sftp Downloader Job for the purpose of downloading and processing
 * the newest file in a given directory on a remote system.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpDownloaderJob {

    private SftpDownloaderJob() {
    }


    /**
     * creates the batch job.
     *
     * @param config
     *            configuration of downloader job.
     * @param client
     *            remote client operations <b>The caller is responsible to
     *            release resources after the Job executes, if applicable.<b>
     * @param remoteConfiguration
     *            remote client storage configuration.
     * @param fileType
     *            type of file that one wants to download.
     * @return Batch Job that can be executed.
     */
    public static BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> makeDownloaderJob(
            final CtlDownloaderJob.Configuration config,
            final RemoteClient client,
            final RemoteConfiguration remoteConfiguration,
            final FileType fileType ) {

        final Processor<FetchedItem<SftpFilename>, SftpFilename, ControlFilePersistenceOutputInfo> downloader =
                Processors.controlledFileWriter( config.getDownloadDirPath(), config.getControlFileEnding(),
                        new SftpDownloadingFileWriterAdapter( client ) );

        final SftpResultFileMover remoteFileMover = new SftpResultFileMover( client, remoteConfiguration.getArchivedFolder() );

        return new BatchJob.Builder<SftpFilename, ControlFilePersistenceOutputInfo>()
                .setFetcher(
                        new SftpOldFilesMovingLatestFileFetcher(
                                client,
                                remoteConfiguration.getSkippedFolder(),
                                remoteConfiguration.getProcessingFolder(),
                                remoteConfiguration.getIncomingFolder(),
                                fileType ) )
                .addListener( new BatchStatisticsLoggingListener<>( "BATCH" ) )
                .addListener( new ItemProgressLoggingListener<>( "ITEM" ) )
                .setProcessor( Processors.compose( remoteFileMover, downloader ) )
                .setProcessingBatchSize( 1 /*No advantage in processing multiple files at once*/ )
                .build();

    }

}
