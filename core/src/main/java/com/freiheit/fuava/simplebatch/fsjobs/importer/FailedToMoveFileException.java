/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 14.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.simplebatch.fsjobs.importer;

/**
 * @author tim.lessner@freiheit.com
 */
public class FailedToMoveFileException extends Exception {
    public FailedToMoveFileException( final String absolutePath ) {
        super( absolutePath );
    }
}
