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

package com.freiheit.fuava.simplebatch;

import com.freiheit.fuava.sftp.ServerConfiguration;
import com.freiheit.fuava.sftp.SftpClient;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.testng.Assert;
import org.testng.annotations.Test;

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
            final ServerConfiguration dummyFalseconfig = new ServerConfiguration( "local,host", 1234, "user", "pass", "/tmp/", "/tmp/process", "/tmp/skipped", "/tmp/archived" );
            final SftpClient client = new SftpClient( dummyFalseconfig );
            client.listFolder( "Freiheit_com" );
        } catch ( final SftpException e ) {
            Assert.assertTrue( e.getCause().toString().contains( "UnknownHostException" ) );
        } catch ( JSchException e ) {
            Assert.assertEquals( e.getMessage(), "java.net.UnknownHostException: local,host" );
        }

    }

}
