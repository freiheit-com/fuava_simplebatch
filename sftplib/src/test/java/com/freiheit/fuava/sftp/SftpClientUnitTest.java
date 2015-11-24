package com.freiheit.fuava.sftp;

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
            final SftpClient client = new SftpClient( "local,host", 1234, "user", "pass" );
            client.listFolder( "Freiheit_com" );
        } catch ( final SftpException e ) {
            Assert.assertTrue( e.getCause().toString().contains( "UnknownHostException" ) );
        } catch ( JSchException e ) {
            Assert.assertEquals( e.getMessage(), "java.net.UnknownHostException: local,host" );
        }

    }

}
