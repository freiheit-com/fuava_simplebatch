/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 25.04.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.sftp.exceptions;

/**
 * Exception to show that no data has been found.
 *
 * @author tim.lessner@freiheit.com
 */
public class NoDataFoundException extends Exception {
    /**
     * Exception to show that no data has been found.
     */
    public NoDataFoundException( final String message ) {
        super( message );
    }
}
