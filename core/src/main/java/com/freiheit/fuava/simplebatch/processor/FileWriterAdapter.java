package com.freiheit.fuava.simplebatch.processor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Charsets;

public interface FileWriterAdapter<Input, Output> extends FileOutputStreamAdapter<Input, Output> {
    @Override
    String getFileName( Result<Input, Output> result );

    @Override
    default public void writeToStream( final java.io.OutputStream outputStream, final Output data ) throws IOException {
        try ( OutputStreamWriter fos = new OutputStreamWriter( outputStream, Charsets.UTF_8 ) ) {
            write( fos, data );
            fos.flush();
        }

    }

    void write( Writer writer, Output data ) throws IOException;
}