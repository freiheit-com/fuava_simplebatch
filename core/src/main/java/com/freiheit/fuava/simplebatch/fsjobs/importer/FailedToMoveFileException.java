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

import java.io.File;

/**
 * @author tim.lessner@freiheit.com
 */
public class FailedToMoveFileException extends Exception {
    public FailedToMoveFileException( final File toMove, final File moveTo ) {

        super( !toMove.exists()
            ? toMove.getAbsolutePath() + " does not exist"
            : !moveTo.exists()
                ? moveTo.getAbsolutePath() + " does not exist"
                : !toMove.exists() && !moveTo.exists()
                    ? "Neither " + toMove.getAbsolutePath() + " exists nor " + moveTo.getAbsolutePath() + " exists"
                    : "" );
    }

    public FailedToMoveFileException( final String msg ) {
        super( msg );
    }
}
