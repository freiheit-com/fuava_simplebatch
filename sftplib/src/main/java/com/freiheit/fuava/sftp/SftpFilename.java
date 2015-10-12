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

import com.freiheit.fuava.sftp.util.FileType;

/**
 * Simple storage for remote file name (and may be in the future sme more
 * information).
 *
 * @author Dmitrijs Barbarins (dmitrijs.barbarins@freiheit.com) on 22.07.15.
 */
public class SftpFilename {
    private final String filename;
    private final String timestamp;
    private final String remoteFullPath;
    private final FileType fileType;

    /**
     * ctor.
     *
     * @param filename
     *            fully qualified file name on the remote server.
     */
    public SftpFilename( final String filename, final String remoteFullPath, final FileType fileType, final String timestamp ) {
        this.timestamp = timestamp;
        this.filename = filename;
        this.fileType = fileType;
        this.remoteFullPath = remoteFullPath;
    }

    public String getRemoteFullPath() {
        return remoteFullPath;
    }

    public String getFilename() {
        return filename;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public FileType getFileType() {
        return fileType;
    }

    @Override
    public String toString() {
        return filename;
    }
}
