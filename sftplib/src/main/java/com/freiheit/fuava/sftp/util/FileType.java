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

package com.freiheit.fuava.sftp.util;

import com.google.common.base.MoreObjects;

import javax.annotation.Nonnull;

/**
 * Describes the type of files to be processed.
 *
 * @author dmitrijs.barbarins@freiheit.com
 */
public class FileType {

    public final static FileType ALL_FILES = new FileType("all","*");

    private final String interfaceName;
    private final String fileIdentifierPattern;
    private final String fileExtention;
    private final String okFileExtention;

    public FileType( @Nonnull final String interfaceName, @Nonnull final String fileIdentifier ) {
        this.interfaceName = interfaceName;
        this.fileIdentifierPattern = fileIdentifier;
        this.fileExtention = ".csv";
        this.okFileExtention = ".ok";
    }

    public FileType( @Nonnull final String interfaceName, @Nonnull final String fileIdentifier,
            @Nonnull final String fileExtention, @Nonnull final String okFileExtention ) {
        this.interfaceName = interfaceName;
        this.fileIdentifierPattern = fileIdentifier;
        this.fileExtention = fileExtention;
        this.okFileExtention = okFileExtention;
    }

    /**
     * The interfaceName, i.e., either storedata, hobase, pwhg, ean or all.
     */
    public String getInterfaceName() {
        return interfaceName;
    }

    /**
     * A pattern that matches files of the appropriate interfaceName.
     */
    public String getFileIdentifier() {
        return fileIdentifierPattern;
    }


    public String getExtention() {
        return fileExtention;
    }

    public String getOkFileExtention() {
        return okFileExtention;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper( this )
                .add( "interfaceName", interfaceName )
                .add( "fileIdentifierPattern", fileIdentifierPattern )
                .add( "fileExtention", fileExtention )
                .add( "okFileExtention", okFileExtention )
                .toString();
    }
}