package com.freiheit.fuava.simplebatch.processor;

import java.io.File;
import java.nio.file.Path;

import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;

public final class ToProcessingMover extends AbstractControlledFileMovingProcessor<ControlFile, File> {
    
    private final Path processingDir;

    public ToProcessingMover( final Path downloadDir, final Path processingDir, final Path failedDir ) {
        super( downloadDir, processingDir, failedDir );
        this.processingDir = processingDir;
    }

    @Override
    protected File getOutput( final ControlFile controlFile, final ControlFile output ) {
        return processingDir.resolve( output.getControlledFileRelPath() ).toFile();
    }
}