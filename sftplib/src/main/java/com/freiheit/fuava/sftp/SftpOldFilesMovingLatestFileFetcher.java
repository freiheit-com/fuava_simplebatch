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

import java.util.List;
import java.util.Map;

import com.freiheit.fuava.sftp.util.FileType;
import com.google.common.collect.ImmutableMap;

/**
 *
 * Fetches the data from a given filename that includes a timestamp.
 * The latest available timestamp is resolved to get the latest data. The rest of the data is moved
 * to a skipped directory.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpOldFilesMovingLatestFileFetcher extends SftpOldFilesMovingLatestMultiFileFetcher {

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
        super( remoteClient, skippedFolder, processingFolder, incomingFilesFolder );
        this.fileType = fileType;
    }

    @Override
    protected Map<FileType, List<String>> byType( final List<String> filenames ) {
        // TODO: filter for filetype matching names. Not really needed, because the timestamp extraction will filter again.
        return ImmutableMap.of( this.fileType, filenames );
    }

}
