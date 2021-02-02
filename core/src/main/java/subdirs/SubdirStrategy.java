package subdirs;

import java.nio.file.Path;

/**
 * The SubdirStrategy on how to pick the subsequent directories.
 */
public interface SubdirStrategy {
    /**
     * Prepends a given filename by a folder to gain the full path of the subdirectory.
     */
    Path prependSubdir( String filename );        
}