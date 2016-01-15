package subdirs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class StringSplittingStrategy implements SubdirStrategy {
    private final int numLetters;
    private final int numLevels;
    
    public StringSplittingStrategy( final int numLetters, final int numLevels ) {
        this.numLetters = numLetters;
        this.numLevels = numLevels;
    }
    
    @Override
    public Path prependSubdir( final String filename ) {
        return prependSubdir( filename.replaceAll( "[^0-9a-zA-Z]", "" ).toLowerCase( Locale.US ), filename );
    }
    
    protected Path prependSubdir( final String directoryChars, final String filename ) {
        final List<String> items = new ArrayList<String>();
        String remaining = directoryChars;
        while ( remaining.length() >= numLetters && items.size() < numLevels ) {
            items.add( remaining.substring( 0,  numLetters ) );
            remaining = remaining.substring( numLetters );
        }
        items.add( filename );
        return Paths.get( items.get( 0 ), items.subList( 1, items.size() ).toArray( new String[ items.size() - 1 ] ) );
    }
}