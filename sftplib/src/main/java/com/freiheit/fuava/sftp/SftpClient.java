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

import com.freiheit.fuava.sftp.util.ConvertUtil;
import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.jcraft.jsch.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple SFTP client.
 *
 * @author Dmitrijs Barbarins (dmitrijs.barbarins@freiheit.com) on 22.07.15.
 */
public class SftpClient implements RemoteClient {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger( SftpClient.class );

    private static final String CHANNEL_TYPE_SFTP = "sftp";

    private final String host;
    private final Integer port;
    private final String username;
    private final String password;

    private final SftpServerConfiguration configuration;
    private ChannelSftp sftpChannel;


    public  String getRemoteSystemType() {
        return CHANNEL_TYPE_SFTP;
    }


    @Override public String getHost() {
        return host;
    }

    @Override public Integer getPort() {
        return port;
    }

    @Override public String getUsername() {
        return username;
    }

    @Override public String getPassword() {
        return password;
    }

    SftpServerConfiguration getRemoteConfiguration() {
        return configuration;
    }

    /**
     * ctor.
     *
     * @param configuration
     *            Environment configuration object
     */
    public SftpClient( final String host, final Integer port, final String username, final String password,
            final SftpServerConfiguration configuration  ) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.configuration = configuration;
    }

    /**
     * Lazy channel initializer.
     *
     * @return the initialized sftp channel
     */
    private ChannelSftp channel() throws JSchException {
        if ( sftpChannel == null || !sftpChannel.isConnected() ) {
            sftpChannel = login( createSession() );
        }
        return sftpChannel;
    }

    protected Session createSession() throws JSchException {
        final JSch jsch = new JSch();
        final Session session = jsch.getSession(
                username,
                host,
                port
                );

        //        If this property is set to ``yes'', jsch will never automatically add
        //        host keys to the $HOME/.ssh/known_hosts file, and refuses to connect
        //        to hosts whose host key has changed.  This property forces the user
        //        to manually add all new hosts.  If this property is set to ``no'',
        //        jsch will automatically add new host keys to the user known hosts files.
        session.setConfig( "StrictHostKeyChecking", "no" );
        session.setPassword( password );

        return session;
    }

    protected ChannelSftp login( Session session ) throws JSchException {
        session.connect();
        try {
            session.sendKeepAliveMsg();
        } catch ( final Exception e ) {
            throw new JSchException( "Sending Keep Alive to SFTP Channel failed: ", e );
        }

        final Channel channel = session.openChannel( CHANNEL_TYPE_SFTP );
        channel.connect();
        return (ChannelSftp) channel;
    }

    /**
     * Returns the list of all files in given folder on the remote server.
     *
     * @param pathToFiles
     *            Path on the remote folder
     * @return List of all entry filenames
     * @throws JSchException
     *             is thrown in case there are any problems with the SFTP
     *             channel
     */
    @SuppressWarnings( "unchecked" )
    // justification: we assume 'Object', therefore we are safe, although unchecked.
    public List<String> listFolder( final String pathToFiles ) throws JSchException, SftpException {
        List<ChannelSftp.LsEntry> listOfLsEntries =  ConvertUtil.convertList( channel().ls( pathToFiles ), SftpClient::toLsEntry );
        return listOfLsEntries.stream().map( SftpOldFilesMovingLatestFileFetcher::lsEntryToFilename )
                .collect( Collectors.toList() );
    }

    @CheckForNull
    private static ChannelSftp.LsEntry toLsEntry( final Object object ) {
        if ( object instanceof ChannelSftp.LsEntry ) {
            return (ChannelSftp.LsEntry) object;
        }
        return null;
    }

    /**
     * Disconnect if there is an active session.
     *
     * @throws JSchException
     *             is throw if client fails to disconnect an existing session.
     */
    public void disconnect() throws JSchException {
        if ( sftpChannel != null ) {
            final Session session = sftpChannel.getSession();
            sftpChannel.exit();
            if ( session != null ) {
                session.disconnect();
            }
        }
    }

    /**
     * Download a file.
     *
     * @param file
     *            path to file.
     * @return InputStream of the desired file.
     * @throws SftpException
     *             is thrown in case of errors.
     */
    public InputStream downloadRemoteFile( final String file ) throws JSchException, SftpException {
        return channel().get( file );
    }

    /**
     * Move a file on remote server.
     *
     * @param sourceFile
     *            path to file.
     * @param destinationFile
     *            path to file.
     * @return InputStream of the desired file.
     * @throws SftpException
     *             is thrown in case of errors.
     */
    public void moveFileOnRemoteSystem( final String sourceFile, final String destinationFile ) throws SftpException, JSchException {
        channel().rename( sourceFile, destinationFile );
    }

    /**
     * Delete the file on server.
     *
     * @param filename
     *            file to delete.
     * @throws JSchException
     * @throws SftpException
     */
    public void deleteFile( final String filename ) throws JSchException, SftpException {
        channel().rm( filename );
    }

    /**
     * Moves .ok and data files from given folder to another one. The ok file
     * will be deleted in case of successful data file movement.
     *
     * @param okFile
     *            file name of the .ok file.
     * @param fileType
     *            type of the data file to move.
     * @param fromFolder
     *            folder where the files are located.
     * @param toFolder
     *            folder to move the files to.
     * @return String full path and filename of end position of the data file.
     * @throws SftpException
     *             exception thrown in case of errors.
     */
    public String moveFileAndControlFileFromOneDirectoryToAnother( final String okFile, FileType fileType, final String fromFolder,
            final String toFolder ) throws SftpException {
        // this file is older then the latest one, move it to the skipped folder
        final String dataFilename = FilenameUtil.getDataFileOfOkFile( fileType, okFile );
        if ( dataFilename == null ) {
            throw new SftpException( 1, "Failed to locate data file for .ok file: " + okFile );
        }

        final String origOkFile = fromFolder + okFile;
        final String destOkFile = toFolder + okFile;
        final String origDataFile = fromFolder + dataFilename;
        final String destDataFile = toFolder + dataFilename;

        try {
            // first move the data file
            moveFileOnRemoteSystem( origDataFile, destDataFile );
            LOG.info( "Moved file " + origDataFile + " to folder " + destDataFile );
        } catch ( final JSchException | SftpException e ) {
            throw new SftpException( 1, "Failed to move data file " + origDataFile + " to folder " + destDataFile, e );
        }

        try {
            // then move .ok file
            moveFileOnRemoteSystem( origOkFile, destOkFile );
            LOG.info( "Moved .ok file " + origOkFile );
        } catch ( final JSchException | SftpException e ) {
            throw new SftpException( 1, "Failed to move .ok file after moving the data file " + origDataFile
                    + " to folder " + destDataFile, e );
        }

        return destDataFile;

    }

    /**
     * Create folder if it does not exist yet.
     *
     * @param folderName
     *            remote folder.
     */
    public void createFolderIfNotExist( final String folderName ) throws JSchException, SftpException {
        String[] complPath = folderName.split( "/" );
        channel().cd( "/" );
        for ( String dir : complPath ) {
            if ( StringUtils.isEmpty( dir ) == false ) {
                try {
                    channel().cd( dir );
                } catch ( SftpException e2 ) {
                    channel().mkdir( dir );
                    channel().cd( dir );
                }
            }
        }
    }

}
