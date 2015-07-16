package com.freiheit.fuava.simplebatch.persist;

import java.io.IOException;
import java.io.Writer;

import com.freiheit.fuava.simplebatch.result.Result;

public interface PersistenceAdapter<Input, Output> {
	String getFileName(Result<Input, Output> result);
	void write(Writer writer, Output data) throws IOException;
}