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