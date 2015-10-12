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
    String getLocationFolder();

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






}
