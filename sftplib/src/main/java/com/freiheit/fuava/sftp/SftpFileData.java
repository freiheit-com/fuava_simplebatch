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

import java.io.InputStream;

/**
 * Simple object containing the data received from the sftp server.
 *
 * @author Dmitrijs Barbarins (dmitrijs.barbarins@freiheit.com) on 22.07.15.
 */
public class SftpFileData {

    private final InputStream data;

    /**
     * ctor.
     *
     * @param data input stream received from the remote server.
     */
    public SftpFileData( final InputStream data ) {
        this.data = data;
    }

    public InputStream getData() {
        return data;
    }
}
