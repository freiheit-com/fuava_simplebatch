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


import javax.annotation.Nonnull;

/**
 * Status of status files on the remote server.
 *
 * @author Jochen Oekonomopulos (jochen.oekonomopulos@freiheit.com)
 */
public enum RemoteFileStatus {

    OK( "ok" );

    private final String fileExtension;

    RemoteFileStatus( @Nonnull final String fileExtensionString ) {
        fileExtension = fileExtensionString;
    }

    /**
     * The file extension indicating the status.
     */
    public String getFileExtension() {
        return fileExtension;
    }

}