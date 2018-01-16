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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.sshd.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.UserAuth;
import org.apache.sshd.server.auth.UserAuthNone;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.sftp.SftpSubsystem;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

/**
 * Tests sftp client class.
 *
 * @author Dmitrijs Barbarins (dmitrijs.barbarins@freiheit.com)
 */
@Test
public class SftpClientUnitTest {

    @Test
    public void testFalseHostNameException() {

        try {
            final SftpClient client =
                new SftpClient.Builder()
                    .setHost( "local,host" )
                    .setPort( 1234 )
                    .setUsername( "user" )
                    .setPassword( "pass" )
                    .createSftpClient();
            client.listFolder( "Freiheit_com" );
        } catch ( final SftpException e ) {
            Assert.assertTrue( e.getCause().toString().contains( "UnknownHostException" ) );
        } catch ( JSchException e ) {
            Assert.assertEquals( e.getMessage(), "java.net.UnknownHostException: local,host" );
        }

    }

    @Test
    public void testRetryableKnownHostsfile() throws IOException, InterruptedException, SftpException {

        final SshServer sshd = this.initSFTPServer();
        sshd.start();
        try {
            final SftpClient client =
                new SftpClient.Builder()
                    .setHost( "localhost" )
                    .setPort( sshd.getPort() )
                    .setUsername( "user" )
                    .setPassword( "pass" )
                    .setStrictHostkeyChecking( SftpClient.StrictHostkeyChecking.ON )
                    .setKnownHostsInputStream( Files.newInputStream( Paths.get("/dev/null" ) ) )
                    .createSftpClient();

            try {
                client.listFolder( "foo" );
                Assert.fail( "should always throw an exception" );
            } catch ( final JSchException e ) {
                // This is the expected behaviour. We have an empty known hosts file.
                Assert.assertTrue( e.getMessage().startsWith( "UnknownHostKey" ), String.format( "Expected message \"%s\" to start with \"UnknownHostKey\" ", e.getMessage() ) );
            }
            try {
                client.listFolder( "foo" );
                Assert.fail( "should always throw an exception" );
            } catch ( final JSchException e ) {
                // This is the expected behaviour. We have an empty known hosts file.
                // But if we have read the InputStream we get a different error.
                Assert.assertTrue( e.getMessage().startsWith( "UnknownHostKey" ), String.format( "Expected message \"%s\" to start with \"UnknownHostKey\" ", e.getMessage() ) );
            }
        } finally {
            sshd.stop();
        }
    }

    @Test
    public void testDisabledHostkeyCheckingWithKnownHostsFails() {

        try {
            new SftpClient.Builder()
                .setHost( "localhost" )
                .setPort( 1234 )
                .setUsername( "user" )
                .setStrictHostkeyChecking( SftpClient.StrictHostkeyChecking.OFF )
                .setKnownHostsInputStream( new ByteArrayInputStream( "foo".getBytes( StandardCharsets.UTF_8) ) )
                .createSftpClient();
            Assert.fail( "No exception thrown despite combination of arguments." );
        } catch ( final IllegalArgumentException e ) {
            // pass
        }
    }

    private SshServer initSFTPServer() {
        final SshServer sshd = SshServer.setUpDefaultServer();
        sshd.setPort( 0 );
        sshd.setKeyPairProvider( new SimpleGeneratorHostKeyProvider( "hostkey.ser" ) );

        final List<NamedFactory<UserAuth>> userAuthFactories = ImmutableList.of( new UserAuthNone.Factory() );
        sshd.setUserAuthFactories( userAuthFactories );

        final List<NamedFactory<Command>> namedFactoryList = ImmutableList.of( new SftpSubsystem.Factory() );
        sshd.setSubsystemFactories( namedFactoryList );

        return sshd;
    }

}
