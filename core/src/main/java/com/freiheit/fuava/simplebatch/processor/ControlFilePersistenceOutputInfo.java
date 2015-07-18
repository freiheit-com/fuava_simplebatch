package com.freiheit.fuava.simplebatch.processor;

import java.io.File;

import com.google.common.base.MoreObjects;

public class ControlFilePersistenceOutputInfo {
	private final File ctrlFile;
	public ControlFilePersistenceOutputInfo(File ctrlFile) {
		this.ctrlFile = ctrlFile;
	}
	
	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("Control File", ctrlFile).toString();
	}
}