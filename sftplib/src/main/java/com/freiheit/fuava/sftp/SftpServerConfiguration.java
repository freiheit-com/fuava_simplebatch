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

    private final String host;
    private final Integer port;

    private final String username;
    private final String password;

    private final String remoteFilesLocationFolder;
    private final String remoteProcessingFolder;
    private final String remoteSkippedFolder;
    private final String remoteArchivedFolder;

    /**
     *
     * @param host hostname of sftp server
     * @param port port of sftp server
     * @param username username for sftp access
     * @param password password for sftp access
     * @param remoteFilesLocationFolder location of files located on sftp server
     * @param remoteProcessingFolder location of files being processed on sftp server
     * @param remoteSkippedFolder location of files being skipped on sftp server
     * @param remoteArchivedFolder location of files have been downloaded successfully from sftp server
     *
     */
    public SftpServerConfiguration( final String host, final Integer port, final String username, final String password,
            final String remoteFilesLocationFolder, final String remoteProcessingFolder, final String remoteSkippedFolder,
            final String remoteArchivedFolder ) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;

        this.remoteArchivedFolder = remoteArchivedFolder;
        this.remoteFilesLocationFolder = remoteFilesLocationFolder;
        this.remoteProcessingFolder = remoteProcessingFolder;
        this.remoteSkippedFolder = remoteSkippedFolder;

    }

    public String getHost() {
        return host;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public Integer getPort() {
        return port;
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
