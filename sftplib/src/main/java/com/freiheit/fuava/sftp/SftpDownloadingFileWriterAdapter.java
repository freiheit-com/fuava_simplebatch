/*
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import com.freiheit.fuava.sftp.util.ConvertUtil;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.FileOutputStreamAdapter;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.StringUtils;
import com.freiheit.fuava.simplebatch.util.Sysprops;

/**
 * The file writer adapter streams data and writes it to the predefined downloading directory.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpDownloadingFileWriterAdapter implements FileOutputStreamAdapter<FetchedItem<SftpFilename>, SftpFilename> {
    private final String prefix = "" + System.currentTimeMillis();
    private final AtomicLong counter = new AtomicLong();

    private final RemoteClient remoteClient;
    
    public SftpDownloadingFileWriterAdapter(final RemoteClient remoteClient) {
        this.remoteClient = remoteClient;
    }

    @Override
    public Path prependSubdirs( final String filename ) {
        return Sysprops.SFTP_SUBDIR_STRATEGY.prependSubdir( filename );
    }

    @Override
    public String getFileName( final Result<FetchedItem<SftpFilename>, SftpFilename> result ) {
        final String filename = result.getInput().getValue().getFilename();
        final String count = StringUtils.padStart( Long.toString( counter.incrementAndGet() ), 3, '0' );
        return prefix + "_" + count + "_" + filename;
    }

    /**
     * writes InputStream to OutputStream.
     *
     * @param outputStream data written from the sftp server
     * @throws IOException when streaming fails.
     */
    @Override
    public void writeToStream( final OutputStream outputStream, final SftpFilename file ) throws IOException {
        final InputStream inputStream;
        try {
            inputStream = remoteClient.downloadRemoteFile( file.getRemoteFullPath() );
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
        try {
            ConvertUtil.copyLargeWithLoggingProgress( inputStream, outputStream );
        } finally {
            inputStream.close(); 
        }
    }

}
