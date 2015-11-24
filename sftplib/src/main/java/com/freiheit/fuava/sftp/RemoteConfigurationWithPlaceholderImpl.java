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

import com.freiheit.fuava.sftp.util.RemoteConfiguration;
import com.freiheit.fuava.simplebatch.util.FileUtils;

/**
 * The SFTP-Server Configuration.
 *
 * @author dmitrijs.barbarins@freiheit.com
 */
public class RemoteConfigurationWithPlaceholderImpl implements RemoteConfiguration {


    private final String remoteFilesIncomingFolder;
    private final String remoteProcessingFolder;
    private final String remoteSkippedFolder;
    private final String remoteArchivedFolder;

    /**
     * Server configuration for sftp.
     * NOTE: adds current date to the archived and skipped folders.
     *
     * @param remoteFilesIncomingFolder location of files located on sftp server
     * @param remoteProcessingFolder location of files being processed on sftp server
     * @param remoteSkippedFolder location of files being skipped on sftp server
     * @param remoteArchivedFolder location of files have been downloaded successfully from sftp server
     *
     */
    public RemoteConfigurationWithPlaceholderImpl(final String remoteFilesIncomingFolder, final String remoteProcessingFolder, final String remoteSkippedFolder,
                                                  final String remoteArchivedFolder) {

        this.remoteArchivedFolder = FileUtils.substitutePlaceholder( remoteArchivedFolder );
        this.remoteFilesIncomingFolder = FileUtils.substitutePlaceholder( remoteFilesIncomingFolder );
        this.remoteProcessingFolder = FileUtils.substitutePlaceholder( remoteProcessingFolder );
        this.remoteSkippedFolder = FileUtils.substitutePlaceholder( remoteSkippedFolder );

    }


    public String getIncomingFolder() {
        return remoteFilesIncomingFolder;
    }

    public String getProcessingFolder() {
        return remoteProcessingFolder;
    }

    public String getSkippedFolder() {
        return remoteSkippedFolder;
    }

    public String getArchivedFolder() {
        return remoteArchivedFolder;
    }
}
