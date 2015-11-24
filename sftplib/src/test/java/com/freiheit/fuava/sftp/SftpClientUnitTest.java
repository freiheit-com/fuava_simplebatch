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
