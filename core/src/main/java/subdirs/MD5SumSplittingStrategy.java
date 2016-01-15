package subdirs;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MD5SumSplittingStrategy extends StringSplittingStrategy {
    
    private static final Logger LOG = LoggerFactory.getLogger( MD5SumSplittingStrategy.class );
    private final int minSize;
    
    public MD5SumSplittingStrategy( final int numLetters, final int numLevels ) {
        super( numLetters, numLevels );
        minSize = numLetters * numLevels;
    }
    
    private String getMD5(final String input) {
        try {
            final MessageDigest md = MessageDigest.getInstance( "MD5" );
            final byte[] messageDigest = md.digest( input.getBytes( StandardCharsets.UTF_8 ) );
            final BigInteger number = new BigInteger( 1, messageDigest );
            String hashtext = number.toString( 16 );
            // ensure that the md5 is always at least the size of the intended subdir tree
            while ( hashtext.length() < minSize ) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        }
        catch (final NoSuchAlgorithmException e) {
            LOG.warn( "Strategy " + this + " not possible, because the algorithm does not exist " + e.getMessage(), e );
            return input;
        }
    }
    
    @Override
    public Path prependSubdir( final String filename ) {
        return prependSubdir( getMD5( filename ), filename );
    }
}