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
package com.freiheit.fuava.sftp;

import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.RemoteConfiguration;
import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;

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
     * 
     * @deprecated Use {@link #makeOldFilesMovingLatestFileDownloaderJob(com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob.Configuration, RemoteClient, RemoteConfiguration, FileType)}  instead
     */
    @Deprecated
    public static BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> makeDownloaderJob(
            final CtlDownloaderJob.Configuration config,
            final RemoteClient client,
            final RemoteConfiguration remoteConfiguration,
            final FileType fileType ) {
        return makeOldFilesMovingLatestFileDownloaderJob(config, client, remoteConfiguration, fileType);
    }

    /**
     * creates a batch job that fetches the latest file for a given pattern, 
     * moving older files to a skipped directory.
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
    public static BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> makeOldFilesMovingLatestFileDownloaderJob(
            final CtlDownloaderJob.Configuration config,
            final RemoteClient client,
            final RemoteConfiguration remoteConfiguration,
            final FileType fileType ) {
        return makeDownloaderJob(
                config, client, remoteConfiguration,
                new SftpOldFilesMovingLatestFileFetcher(
                    client,
                    remoteConfiguration.getSkippedFolder(),
                    remoteConfiguration.getProcessingFolder(),
                    remoteConfiguration.getIncomingFolder(),
                    fileType 
                ) );
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
     * @param fileFetcher
     *            Provides the names of the Files that should be fetched
     * @return Batch Job that can be executed.
     */
    public static BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> makeDownloaderJob(
            final CtlDownloaderJob.Configuration config,
            final RemoteClient client,
            final RemoteConfiguration remoteConfiguration,
            final Fetcher<SftpFilename> fileFetcher ) {

        final Processor<FetchedItem<SftpFilename>, SftpFilename, ControlFilePersistenceOutputInfo> downloader =
                Processors.controlledFileWriter( config.getDownloadDirPath(), config.getControlFileEnding(),
                        new SftpDownloadingFileWriterAdapter( client ) );

        final SftpResultFileMover remoteFileMover = new SftpResultFileMover( client, remoteConfiguration.getArchivedFolder() );

        return new BatchJob.Builder<SftpFilename, ControlFilePersistenceOutputInfo>()
                .setFetcher(fileFetcher )
                .addListener( new BatchStatisticsLoggingListener<>( CtlDownloaderJob.LOG_NAME_BATCH ) )
                .addListener( new ItemProgressLoggingListener<>( CtlDownloaderJob.LOG_NAME_ITEM ) )
                .setProcessor( Processors.compose( remoteFileMover, downloader ) )
                .setProcessingBatchSize( 1 /*No advantage in processing multiple files at once*/ )
                .build();

    }
}
