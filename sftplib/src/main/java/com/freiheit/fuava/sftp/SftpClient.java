/**
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

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.CheckForNull;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;
import com.freiheit.fuava.sftp.util.ConvertUtil;
import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.google.common.base.Preconditions;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/**
 * Simple SFTP client.
 *
 * @author Dmitrijs Barbarins (dmitrijs.barbarins@freiheit.com) on 22.07.15.
 */
public class SftpClient implements RemoteClient {

    /**
     * Whether to use StrictHostKeyChecking or not.
     *
     * If enabled it will be impossible to connect to any host not present (or with another key than) in the
     * known hosts file (which you can provide yourself, if you don't want to use the system default).
     *
     * You should have good reasons to disable this.
     */
    public enum StrictHostkeyChecking { ON, OFF }

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger( SftpClient.class );

    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 6000; // ms

    private static final String CHANNEL_TYPE_SFTP = "sftp";

    private final String host;
    private final Integer port;
    private final String username;
    private final String password;
    private final int socketTimeoutMs;
    private final InputStream knownHostsInputStream;
    private final StrictHostkeyChecking strictHostkeyChecking;

    private Session session = null;

    private ChannelSftp sftpChannel;


    /**
     * ctor.
     *
     */
    public SftpClient( final String host, final Integer port, final String username, final String password ) {
        this( host, port, username, password, DEFAULT_SOCKET_TIMEOUT_MS );
    }

    public SftpClient( final String host, final Integer port, final String username, final String password, final int socketTimeoutMs ) {
        this( host, port, username, password, socketTimeoutMs, null, StrictHostkeyChecking.ON );
    }

    SftpClient(
        final String host,
        final Integer port,
        final String username,
        final String password,
        final int socketTimeoutMs,
        final InputStream knownHostsInputStream,
        final StrictHostkeyChecking strictHostkeyChecking
    ) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.socketTimeoutMs = socketTimeoutMs;
        this.knownHostsInputStream = knownHostsInputStream;
        this.strictHostkeyChecking = strictHostkeyChecking;
    }

    public String getRemoteSystemType() {
        return CHANNEL_TYPE_SFTP;
    }



    /**
     * Lazy channel initializer.
     *
     * @return the initialized sftp channel
     */
    private ChannelSftp channel() throws JSchException {
        if ( sftpChannel == null || !sftpChannel.isConnected() ) {
            sftpChannel = login( getSession() );
        }
        return sftpChannel;
    }

    protected synchronized Session getSession() throws JSchException {
        if( this.session == null ){
            this.session = createSession();
        }
        return this.session;
    }

    protected Session createSession() throws JSchException {
        final JSch jsch = new JSch();
        final Session session = jsch.getSession( username, host, port );

        if ( this.knownHostsInputStream != null ) {
            jsch.setKnownHosts( this.knownHostsInputStream );
        }

        if ( this.strictHostkeyChecking == StrictHostkeyChecking.OFF ) {
            LOG.warn( "Session created with StrictHostKeyChecking disabled." );
            // If this property is set to ``yes'', jsch will never automatically add
            // host keys to the $HOME/.ssh/known_hosts file, and refuses to connect
            // to hosts whose host key has changed.  This property forces the user
            // to manually add all new hosts.  If this property is set to ``no'',
            // jsch will automatically add new host keys to the user known hosts files.
            session.setConfig( "StrictHostKeyChecking", "no" );
        }
        session.setPassword( password );

        // socket timeout in milliseconds
        session.setTimeout( socketTimeoutMs );

        return session;
    }

    protected ChannelSftp login( final Session session ) throws JSchException {
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
    @Override
    @SuppressWarnings( "unchecked" )
    // justification: we assume 'Object', therefore we are safe, although unchecked.
    public List<String> listFolder( final String pathToFiles ) throws JSchException, SftpException {
        final List<ChannelSftp.LsEntry> listOfLsEntries =  ConvertUtil.convertList( channel().ls( pathToFiles ), SftpClient::toLsEntry );
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
    @Override
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
     * @throws SftpException
     *             is thrown in case of errors.
     */
    @Override
    public void moveFileOnRemoteSystem( final String sourceFile, final String destinationFile ) throws SftpException, JSchException {
        channel().rename( sourceFile, destinationFile );
    }

    /**
     * Delete the file on server.
     *
     * @param filename
     *            file to delete.
     * @throws JSchException is thrown if client fails to disconnect an existing session.
     *
     * @throws SftpException is thrown in case of errors.
     */
    @Override
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
    @Override
    public String moveFileAndControlFileFromOneDirectoryToAnother( final String okFile, final FileType fileType, final String fromFolder,
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
    @Override
    public void createFolderIfNotExist( final String folderName ) throws JSchException, SftpException {
        final String[] complPath = folderName.split( "/" );
        channel().cd( "/" );
        for ( final String dir : complPath ) {
            if ( !StringUtils.isEmpty( dir ) ) {
                try {
                    channel().cd( dir );
                } catch ( final SftpException e2 ) {
                    channel().mkdir( dir );
                    channel().cd( dir );
                }
            }
        }
    }

    /**
     * A builder for SftpClients for your convenience.
     */
    public static class Builder {

        private String host;
        private Integer port;
        private String username;
        private String password;
        private int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
        private InputStream knownHostsInputStream = null;
        private SftpClient.StrictHostkeyChecking strictHostkeyChecking = StrictHostkeyChecking.ON;

        public Builder setHost( final String host ) {
            this.host = host;
            return this;
        }

        public Builder setPort( final Integer port ) {
            this.port = port;
            return this;
        }

        public Builder setUsername( final String username ) {
            this.username = username;
            return this;
        }

        public Builder setPassword( final String password ) {
            this.password = password;
            return this;
        }

        public Builder setSocketTimeoutMs( final int socketTimeoutMs ) {
            this.socketTimeoutMs = socketTimeoutMs;
            return this;
        }

        public Builder setKnownHostsInputStream( final InputStream knownHostsInputStream ) {
            this.knownHostsInputStream = knownHostsInputStream;
            return this;
        }

        public Builder setStrictHostkeyChecking( final SftpClient.StrictHostkeyChecking strictHostkeyChecking ) {
            this.strictHostkeyChecking = strictHostkeyChecking;
            return this;
        }

        public SftpClient createSftpClient() {
            Preconditions.checkNotNull( this.host, "You have to provide a host" );
            Preconditions.checkNotNull( this.port, "You have to provide a port" );
            Preconditions.checkNotNull( this.username, "You have to provide a username" );
            if ( this.strictHostkeyChecking == StrictHostkeyChecking.OFF ) {
                Preconditions.checkArgument( this.knownHostsInputStream == null,
                    "You disabled StrictHostKeyChecking but provided knownHosts (which would have no effect)." );
            }
            return new SftpClient( host, port, username, password, socketTimeoutMs, knownHostsInputStream, strictHostkeyChecking );
        }
    }

}
