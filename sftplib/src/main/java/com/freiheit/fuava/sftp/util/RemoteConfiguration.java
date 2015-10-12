package com.freiheit.fuava.sftp.util;

/**
 * Configuration for directories on SFTP server for a given file type.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public interface RemoteConfiguration {

    /**
     * Account user name of the remote system
     *
     * @return user name for the remote system.
     */

    String getUsername();

    /**
     * Returns password for remote system.
     *
     * @return password for remote system.
     */
    String getPassword();

    /**
     *
     * Returns Host IP of remote system.
     *
     * @return host ip of remote system.
     */
    String getHost();

    /**
     * Port for access to remote system.
     *
     * @return port of remote system.
     */

    Integer getPort();

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
