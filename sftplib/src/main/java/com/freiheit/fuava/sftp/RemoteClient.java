package com.freiheit.fuava.sftp;

import com.freiheit.fuava.sftp.util.FileType;

import java.io.InputStream;
import java.util.List;

/**
 * Interface for the remote client operations.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public interface RemoteClient {

    /**
     * Returns input stream from a given file path on the remote system in order to download the file. The caller must ensure that the stream is closed.
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
    List<String> listFolder( String pathToFiles ) throws Exception;

    /**
     * Moves files defined in file type on remote system from one folder to another.
     *
     * @param pathToControlFile path to control file that ensures that a complete file is located on the remote system
     * @param fileType file type, especially name setting and ending (i.e. xls, csv etc.)
     * @param sourceFolder folder on remote system where the data shall be moved from.
     * @param destinationFolder folder on then remote system where the data shall be moved to.
     * @return String of new location.
     */
    String moveFileAndControlFileFromOneDirectoryToAnother( String pathToControlFile,
            FileType fileType,
            String sourceFolder,
            String destinationFolder ) throws Exception;




}
