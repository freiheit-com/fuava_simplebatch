package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;

import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;

public final class ToArchiveDirMover<Data> extends AbstractControlledFileMovingProcessor<Data, Data> {
    
    public ToArchiveDirMover( final Path archivedDir, final Path failedDir ) {
        super( archivedDir, failedDir );
    }

    @Override
    protected Data getOutput( final ControlFile controlFile, final Data data ) {
        return data;
    }
}