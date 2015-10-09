package com.freiheit.fuava.sftp;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.sftp.util.RemoteConfiguration;

import java.io.InputStream;

/**
 * Standard Sftp Downloader Job for the purpose of downloading and processing
 * the newest file in a given directory.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpDownloaderJob {

    private SftpDownloaderJob() {

    }

    private static CtlDownloaderJob.Configuration createDownloadConfig( final String downloadingDir ) {
        return new CtlDownloaderJob.ConfigurationImpl().setDownloadDirPath(
                downloadingDir ).setControlFileEnding( ".ctl" );
    }

    private static CtlDownloaderJob makeDownloaderJob(
            final CtlDownloaderJob.Configuration config,
            final SftpClient client,
            final RemoteConfiguration remoteConfiguration ) {
            final Processor<FetchedItem<SftpFilename>, InputStream, ControlFilePersistenceOutputInfo>
                filePersister = Processors.controlledFileWriter( config.getDownloadDirPath(), config.getControlFileEnding(),
                new SftpDownloadLatestFileFileWriterAadapter() );

        return new CtlDownloaderJob.Builder<SftpFilename, InputStream>()
                .setConfiguration( config )
                .setIdsFetcher(
                        new SftpDownloadLatestFileFetcher(
                                client,
                                remoteConfiguration.getLocationFolder(),
                                remoteConfiguration.getSkippedFolder(),
                                remoteConfiguration.getProcessingFolder(),
                                remoteConfiguration.getFileType() ) )
                .setDownloader(
                        new SftpDownloadLatestFileDownloader( client, remoteConfiguration.getArchivedFolder() ) )

                .build( Processors.compose( new SftpResultFileMover( client, remoteConfiguration.getArchivedFolder() ),
                        filePersister ) );

    }

}
