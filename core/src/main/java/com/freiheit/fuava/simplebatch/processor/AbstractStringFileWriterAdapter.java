package com.freiheit.fuava.simplebatch.processor;

import java.io.IOException;
import java.io.Writer;

import com.freiheit.fuava.simplebatch.result.Result;

public class AbstractStringFileWriterAdapter<Input, Output> implements FileWriterAdapter<Input, Output> {

    @Override
    public String getFileName( final Result<Input, Output> result ) {
        final Input input = result.getInput();
        return input == null
            ? "null"
            : input.toString();
    }

    @Override
    public void write( final Writer writer, final Output data ) throws IOException {
        writer.write( data == null
            ? null
            : data.toString() );
    }

}
