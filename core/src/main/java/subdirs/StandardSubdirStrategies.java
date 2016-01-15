package subdirs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum StandardSubdirStrategies implements SubdirStrategy {
    
    NONE {
        @Override
        public Path prependSubdir( final String filename ) {
            return Paths.get( filename );
        }
    },
    
    HOURS_MINUTES {
        @Override
        public Path prependSubdir( final String filename ) {
            final LocalTime now = LocalTime.now();
            return Paths.get( String.format( "%02d", now.getHour() ), String.format( "%02d", now.getMinute() ), filename );
        }
    },
    
    THREE_LETTERS_2_LEVELS {
        private final StringSplittingStrategy strategy = new StringSplittingStrategy( 3, 2 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },
    
    THREE_LETTERS_3_LEVELS {
        private final StringSplittingStrategy strategy = new StringSplittingStrategy( 3, 3 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },

    MD5_ONE_LETTER {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 1, 1 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },
    
    MD5_ONE_LETTER_TWO_DIRS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 1, 2 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },

    MD5_ONE_LETTER_THREE_DIRS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 1, 3 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },

    MD5_TWO_LETTERS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 2, 1 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },

    MD5_TWO_LETTERS_TWO_DIRS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 2, 2 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },

    MD5_TWO_LETTERS_THREE_DIRS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 2, 3 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },

    MD5_THREE_LETTERS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 3, 1 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },
        
    MD5_THREE_LETTERS_2_LEVELS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 3, 2 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    },
    
    MD5_THREE_LETTERS_3_LEVELS {
        private final MD5SumSplittingStrategy strategy = new MD5SumSplittingStrategy( 3, 3 );
        @Override
        public Path prependSubdir( final String filename ) {
            return strategy.prependSubdir( filename );
        }
    };
    
    private static final Logger LOG = LoggerFactory.getLogger( StandardSubdirStrategies.class );
    
    public static StandardSubdirStrategies getInstance( final String value ) {
        try {
            return valueOf( value );
        } catch ( final IllegalArgumentException e ) {
            LOG.warn( "Unknown Subdir Strategy " + value + " - Use one of " + Arrays.toString( values() ) );
            return NONE;
        }
    }
}