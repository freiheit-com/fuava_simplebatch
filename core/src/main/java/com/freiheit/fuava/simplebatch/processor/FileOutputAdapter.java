package com.freiheit.fuava.simplebatch.processor;

import java.io.IOException;
import java.io.OutputStream;

import com.freiheit.fuava.simplebatch.result.Result;

public interface FileOutputAdapter<Input, Output> {
    String getFileName( Result<Input, Output> result );

    void writeToStream( OutputStream outputStream, Output data ) throws IOException;
}