/*
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.sftp.util;

import javax.annotation.Nonnull;

/**
 * Describes the type of files to be processed.
 *
 * @author dmitrijs.barbarins@freiheit.com
 */
public class FileType {
    public static final FileType ALL_FILES = new FileType( "all", "*" );

    private final String interfaceName;
    private final String fileIdentifierPattern;
    private final String fileExtention;
    private final String okFileExtention;

    /**
     *  Constructs a file type from interface name and file identifier.
     *
     * @param interfaceName
     *          the name of the interface where the data is from. That is important if data comes from different sources.
     *          Inn general this is the beginning pattern of the file name, i.e. mcrm, crm etc.
     * @param fileIdentifier
     *          everything that is between the interface name and the first number that appears in the file name.
     *          I.e. crm_test_data_{DATE} (see also {@link FilenameUtil#DATE_TIME_PATTERN }
     *          Usually, the first number indicates a date starting. The pattern of the date following has to be:
     *          YYYYMMDD_HHMMSS, followd by the file extension or okFileExtension
     */
    public FileType( @Nonnull final String interfaceName, @Nonnull final String fileIdentifier ) {
        this.interfaceName = interfaceName;
        this.fileIdentifierPattern = fileIdentifier;
        this.fileExtention = ".csv";
        this.okFileExtention = ".ok";
    }

    /**
     * Constructs a file type from interface name, file identifier, file extension and ok file extension.

     *
     * @param interfaceName
     *          the name of the interface where the data is from. That is important if data comes from different sources.
     *          Inn general this is the beginning pattern of the file name, i.e. mcrm, crm etc.
     * @param fileIdentifier
     *          everything that is between the interface name and the first number that appears in the file name.
     *          I.e. crm_test_data_{DATE} (see also {@link FilenameUtil#DATE_TIME_PATTERN }
     *          Usually, the first number indicates a date starting. The pattern of the date following has to be:
     *          YYYYMMDD_HHMMSS, followd by the
     * @param fileExtention or
     * @param okFileExtention of the file.
     */
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
        return "FileType{" +
                "interfaceName='" + interfaceName + '\'' +
                ", fileIdentifierPattern='" + fileIdentifierPattern + '\'' +
                ", fileExtention='" + fileExtention + '\'' +
                ", okFileExtention='" + okFileExtention + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = ( prime * result ) + ( ( fileExtention == null )
            ? 0
            : fileExtention.hashCode() );
        result = ( prime * result ) + ( ( fileIdentifierPattern == null )
            ? 0
            : fileIdentifierPattern.hashCode() );
        result = ( prime * result ) + ( ( interfaceName == null )
            ? 0
            : interfaceName.hashCode() );
        result = ( prime * result ) + ( ( okFileExtention == null )
            ? 0
            : okFileExtention.hashCode() );
        return result;
    }

    @Override
    public boolean equals( final Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( getClass() != obj.getClass() ) {
            return false;
        }
        final FileType other = (FileType) obj;
        if ( fileExtention == null ) {
            if ( other.fileExtention != null ) {
                return false;
            }
        } else if ( !fileExtention.equals( other.fileExtention ) ) {
            return false;
        }
        if ( fileIdentifierPattern == null ) {
            if ( other.fileIdentifierPattern != null ) {
                return false;
            }
        } else if ( !fileIdentifierPattern.equals( other.fileIdentifierPattern ) ) {
            return false;
        }
        if ( interfaceName == null ) {
            if ( other.interfaceName != null ) {
                return false;
            }
        } else if ( !interfaceName.equals( other.interfaceName ) ) {
            return false;
        }
        if ( okFileExtention == null ) {
            if ( other.okFileExtention != null ) {
                return false;
            }
        } else if ( !okFileExtention.equals( other.okFileExtention ) ) {
            return false;
        }
        return true;
    }
}