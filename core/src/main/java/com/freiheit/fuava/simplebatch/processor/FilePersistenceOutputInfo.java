package com.freiheit.fuava.simplebatch.processor;

import java.io.File;

import com.google.common.base.MoreObjects;

public class FilePersistenceOutputInfo {
	private final File dataFile;
	public FilePersistenceOutputInfo(File dataFile) {
		this.dataFile = dataFile;
	}
	
	
	public File getDataFile() {
		return dataFile;
	}
	
	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("Data File", dataFile).toString();
	}
}