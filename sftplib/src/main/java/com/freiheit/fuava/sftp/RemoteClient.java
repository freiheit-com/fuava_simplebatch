/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 09.10.15 by thomas.ostendorf@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */


package com.freiheit.fuava.sftp;

import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.RemoteConfiguration;

import java.io.InputStream;
import java.util.List;

/**
 * Interface for the remote client operations.
 *
 * @param <FOLDER> The type of a folder object.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public interface RemoteClient<FOLDER> {

    /**
     * type of the remote system, i.e. "sftp", "opsenssh" etc
     *
     *  @return name of remote system.
     */

    String getRemoteSystemType();

    /**
     * Returns the remote configuration for the remote system.
     *
     * @return configuration of remote system.
     */
    RemoteConfiguration getRemoteConfiguration();

    /**
     * Returns input stream from a given file path on the remote system in order to download the file.
     *
     */
    InputStream downloadRemoteFile( String pathToFile ) throws Exception;

    /**
     *  Moves file on remote system from one to another directory.
     */
    void moveFileOnRemoteSystem( String sourcePath, String destinationPath ) throws Exception;

    /**
     *  Deletes file on remote system.
     */
    void deleteFile( String pathOfFileToDelete ) throws Exception;

    /**
     * Creates folder on remote system if it does not exist yet.
     */
    void createFolderIfNotExist( String folderNameToCreate ) throws Exception;

    /**
     * List of objects in a given directory on the remote system.
     *
     */
    List<FOLDER> listFolder( String pathToFiles ) throws Exception;

    /**
     * Moves files defined in file type on remote system from one folder to another.
     *
     * @param pathToControlFile path to control file that ensures that a complete file is located on the remote system
     * @param fileType file type, especially name setting and ending (i.e. xls, csv etc.)
     * @param sourceFolder folder on remote system where the data shall be moved from.
     * @param destinationFolder folder on then remote system where the data shall be moved to.
     * @return
     */
    String moveFileAndControlFileFromOneDirectoryToAnother( String pathToControlFile,
            FileType fileType,
            String sourceFolder,
            String destinationFolder) throws Exception;




}
