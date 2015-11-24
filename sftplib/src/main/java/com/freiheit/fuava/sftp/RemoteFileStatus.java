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