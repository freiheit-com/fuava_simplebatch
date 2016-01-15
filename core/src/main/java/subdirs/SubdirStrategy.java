package subdirs;

import java.nio.file.Path;

public interface SubdirStrategy {
    Path prependSubdir( String filename );        
}