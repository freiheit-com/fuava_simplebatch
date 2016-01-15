package com.freiheit.fuava.simplebatch.fetch;

import java.nio.file.Path;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

public final class DownloadDir {
    private final Path path;
    private final String prefix;
    private final String suffix;
    
    public DownloadDir( final Path path, @Nullable final String prefix, final String suffix ) {
        this.path = Preconditions.checkNotNull( path );
        this.prefix = prefix;
        this.suffix = Preconditions.checkNotNull( suffix );
    }
    
    public Path getPath() {
        return path;
    }
    
    @CheckForNull
    public String getPrefix() {
        return prefix;
    }
    
    public String getSuffix() {
        return suffix;
    }
    
    @Override
    public String toString() {
        return path.toString() + " - " + prefix + " - " + suffix;
    }
}