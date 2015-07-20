package com.freiheit.fuava.simplebatch.processor;

final class ControlFilePersistenceConfigImpl implements
        ControlFilePersistence.Configuration {
    private final String dirName;
    private final String controlFileEnding;

    ControlFilePersistenceConfigImpl( final String dirName,
            final String controlFileEnding ) {
        this.dirName = dirName;
        this.controlFileEnding = controlFileEnding;
    }

    @Override
    public String getDownloadDirPath() {
        return dirName;
    }

    @Override
    public String getControlFileEnding() {
        return controlFileEnding;
    }
}