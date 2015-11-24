package com.freiheit.fuava.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import com.freiheit.fuava.sftp.util.ConvertUtil;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.FileOutputStreamAdapter;
import com.freiheit.fuava.simplebatch.result.Result;

/**
 * The file writer adapter streams data and writes it to the predefined downloading directory.
 *
 * @author Thomas Ostendorf (thomas.ostendorf@freiheit.com)
 */
public class SftpDownloadingFileWriterAdapter implements FileOutputStreamAdapter<FetchedItem<SftpFilename>, SftpFilename> {
    private final String prefix = "" + System.currentTimeMillis();
    private final AtomicLong counter = new AtomicLong();

    private final RemoteClient remoteClient;
    
    public SftpDownloadingFileWriterAdapter(RemoteClient remoteClient) {
        this.remoteClient = remoteClient;
    }

    @Override
    public String getFileName( final Result<FetchedItem<SftpFilename>, SftpFilename> result ) {
        final String filename = result.getInput().getValue().getFilename();
        final String count = com.google.common.base.Strings.padStart( Long.toString( counter.incrementAndGet() ), 3, '0' );
        return prefix + "_" + count + "_" + filename;
    }

    /**
     * writes InputStream to OutputStream.
     *
     * @param outputStream data written from the sftp server
     * @param inputStream data from the sftp server
     * @throws IOException when streaming fails.
     */
    @Override
    public void writeToStream( final OutputStream outputStream, final SftpFilename file ) throws IOException {
        final InputStream inputStream;
        try {
            inputStream = remoteClient.downloadRemoteFile( file.getRemoteFullPath() );
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        try {
            ConvertUtil.copyLargeWithLoggingProgress( inputStream, outputStream );
        } finally {
            inputStream.close(); 
        }
    }

}
