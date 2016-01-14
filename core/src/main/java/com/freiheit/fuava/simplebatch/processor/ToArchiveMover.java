package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;

import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;

public final class ToArchiveMover<Data> extends AbstractControlledFileMovingProcessor<Data, Data> {
    
    public ToArchiveMover( final Path processingDir, final Path archivedDir, final Path failedDir ) {
        super( processingDir, archivedDir, failedDir );
    }

    @Override
    protected Data getOutput( final ControlFile controlFile, final Data data ) {
        return data;
    }
}