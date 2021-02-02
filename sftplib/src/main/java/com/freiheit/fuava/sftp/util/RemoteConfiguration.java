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

/**
 * Configuration for directories on SFTP server for a given file type.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public interface RemoteConfiguration {

    /**
     * Directory on the sftp where the requested files are located.
     *
     * @return location folder path.
     */
    String getIncomingFolder();

    /**
     * Directory where the files are located that have already been processed.
     *
     * @return archived folder path.
     */

    String getArchivedFolder();

    /**
     * Directory where the older files that are skipped are located.
     *
     * @return skipped folder path.
     */
    String getSkippedFolder();

    /**
     * Directory where the files that are currently processed are located.
     *
     * @return processing folder path.
     */

    String getProcessingFolder();

    /**
     * Toggle whether files are moved to the processing folder
     */
    default boolean isMoveToProcessing() {
        return true;
    }

}
