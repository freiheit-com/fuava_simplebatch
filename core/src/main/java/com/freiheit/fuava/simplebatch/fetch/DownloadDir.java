package com.freiheit.fuava.simplebatch.fetch;

import java.nio.file.Path;
import java.util.Objects;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

public final class DownloadDir {
    private final Path path;
    private final String prefix;
    private final String suffix;
    
    public DownloadDir( final Path path, @Nullable final String prefix, final String suffix ) {
        this.path = Objects.requireNonNull( path );
        this.prefix = prefix;
        this.suffix = Objects.requireNonNull( suffix );
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