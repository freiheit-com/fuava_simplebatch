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

package com.freiheit.fuava.sftp.exceptions;

/**
 * Indicates, that a File has already been processed successfully.
 *
 * @author Jochen Oekonomopulos (jochen.oekonomopulos@freiheit.com)
 */
public class FileNotReadyException extends Exception {

    /**
     * Constructs a new exception with the specified detail message. The cause
     * is not initialized.
     *
     * @param message
     *              the detail message
     */
    public FileNotReadyException( final String message ) {
        super( message );
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message
     *            the detail message
     * @param cause
     *            the cause
     */
    public FileNotReadyException( final String message, final Exception cause ) {
        super( message, cause );
    }
}
