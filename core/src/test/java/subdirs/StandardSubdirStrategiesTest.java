package subdirs;

import java.time.LocalDateTime;

import com.freiheit.fuava.simplebatch.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StandardSubdirStrategiesTest {
    @DataProvider
    public Object[][] testStrategies() {
        final LocalDateTime now = LocalDateTime.now();
        final String hours = StringUtils.padStart( Integer.toString( now.getHour() ) , 2, '0' );
        final String minutes = StringUtils.padStart( Integer.toString( now.getMinute() ) , 2, '0' );
        return new Object[][] {
            { StandardSubdirStrategies.NONE, "foo.txt", "foo.txt" },
            { StandardSubdirStrategies.NONE, "1.ctl", "1.ctl" },
            { StandardSubdirStrategies.HOURS_MINUTES, "foo.ctl", hours + "/" + minutes + "/foo.ctl" },
            { StandardSubdirStrategies.THREE_LETTERS_2_LEVELS, "tx", "tx" },
            { StandardSubdirStrategies.THREE_LETTERS_2_LEVELS, "l.t", "l.t" },
            { StandardSubdirStrategies.THREE_LETTERS_2_LEVELS, "l.tx", "ltx/l.tx" },
            { StandardSubdirStrategies.THREE_LETTERS_2_LEVELS, "txt", "txt/txt" },
            { StandardSubdirStrategies.THREE_LETTERS_2_LEVELS, "1.txt", "1tx/1.txt" },
            { StandardSubdirStrategies.THREE_LETTERS_2_LEVELS, "abc.txt", "abc/txt/abc.txt" },
            { StandardSubdirStrategies.THREE_LETTERS_2_LEVELS, "thisIsATest.txt", "thi/sis/thisIsATest.txt" },
            { StandardSubdirStrategies.THREE_LETTERS_3_LEVELS, "thisIsATest.txt", "thi/sis/ate/thisIsATest.txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_2_LEVELS, "tx", "7da/685/tx" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_2_LEVELS, "l.t", "c59/96f/l.t" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_2_LEVELS, "l.tx", "100/af8/l.tx" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_2_LEVELS, "txt", "c78/24f/txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_2_LEVELS, "1.txt", "dd7/ec9/1.txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_2_LEVELS, "abc.txt", "56b/6f0/abc.txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_2_LEVELS, "thisIsATest.txt", "bf2/72b/thisIsATest.txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS_3_LEVELS, "thisIsATest.txt", "bf2/72b/8c6/thisIsATest.txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS, "tx", "7da/tx" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS, "l.t", "c59/l.t" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS, "l.tx", "100/l.tx" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS, "txt", "c78/txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS, "1.txt", "dd7/1.txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS, "abc.txt", "56b/abc.txt" },
            { StandardSubdirStrategies.MD5_THREE_LETTERS, "thisIsATest.txt", "bf2/thisIsATest.txt" },
            { StandardSubdirStrategies.MD5_ONE_LETTER, "tx", "7/tx" },
            { StandardSubdirStrategies.MD5_ONE_LETTER, "l.t", "c/l.t" },
            { StandardSubdirStrategies.MD5_ONE_LETTER, "l.tx", "1/l.tx" },
            { StandardSubdirStrategies.MD5_ONE_LETTER, "txt", "c/txt" },
            { StandardSubdirStrategies.MD5_ONE_LETTER, "1.txt", "d/1.txt" },
            { StandardSubdirStrategies.MD5_ONE_LETTER, "thisIsATest.txt", "b/thisIsATest.txt" },
            { StandardSubdirStrategies.MD5_ONE_LETTER, "abc.txt", "5/abc.txt" },
            { StandardSubdirStrategies.MD5_ONE_LETTER_TWO_DIRS, "abc.txt", "5/6/abc.txt" },
            { StandardSubdirStrategies.MD5_ONE_LETTER_THREE_DIRS, "abc.txt", "5/6/b/abc.txt" },
            { StandardSubdirStrategies.MD5_TWO_LETTERS, "abc.txt", "56/abc.txt" },
            { StandardSubdirStrategies.MD5_TWO_LETTERS_TWO_DIRS, "abc.txt", "56/b6/abc.txt" },
            { StandardSubdirStrategies.MD5_TWO_LETTERS_THREE_DIRS, "abc.txt", "56/b6/f0/abc.txt" },
        };
    }

    @Test( dataProvider = "testStrategies" )
    public void testStrategy(final SubdirStrategy strategy, final String input, final String expectedPath) {
        Assert.assertEquals( strategy.prependSubdir( input ).toString(), expectedPath );
    }
}
