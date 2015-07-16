package com.freiheit.fuava.simplebatch.persist;

import java.io.IOException;
import java.io.Writer;

import com.freiheit.fuava.simplebatch.result.Result;

public class AbstractStringPersistenceAdapter<Input, Output> implements PersistenceAdapter<Input, Output>{

	@Override
	public String getFileName(Result<Input, Output> result) {
		Input input = result.getInput();
		return input == null ? "null" : input.toString();
	}

	@Override
	public void write(Writer writer, Output data) throws IOException {
		writer.write( data == null ? null : data.toString() );		
	}

}
