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
