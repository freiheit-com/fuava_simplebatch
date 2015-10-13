/*
 *
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created by Dmitrijs Barbarins (dmitrijs.barbarins@freiheit.com)
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 *
 */

package com.freiheit.fuava.sftp;

import com.freiheit.fuava.sftp.util.RemoteConfiguration;

/**
 * The SFTP-Server Configuration.
 *
 * @author dmitrijs.barbarins@freiheit.com
 */
public class SftpServerConfiguration implements RemoteConfiguration {


    private final String remoteFilesLocationFolder;
    private final String remoteProcessingFolder;
    private final String remoteSkippedFolder;
    private final String remoteArchivedFolder;

    /**
     * server configuration for sftp.
     *
     * @param remoteFilesLocationFolder location of files located on sftp server
     * @param remoteProcessingFolder location of files being processed on sftp server
     * @param remoteSkippedFolder location of files being skipped on sftp server
     * @param remoteArchivedFolder location of files have been downloaded successfully from sftp server
     *
     */
    public SftpServerConfiguration( final String remoteFilesLocationFolder, final String remoteProcessingFolder, final String remoteSkippedFolder,
            final String remoteArchivedFolder ) {

        this.remoteArchivedFolder = remoteArchivedFolder;
        this.remoteFilesLocationFolder = remoteFilesLocationFolder;
        this.remoteProcessingFolder = remoteProcessingFolder;
        this.remoteSkippedFolder = remoteSkippedFolder;

    }


    public String getLocationFolder() {
        return remoteFilesLocationFolder;
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
