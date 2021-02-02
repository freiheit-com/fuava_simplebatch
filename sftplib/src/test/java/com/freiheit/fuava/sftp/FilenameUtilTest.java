/*
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.sftp;

import com.freiheit.fuava.sftp.util.FilenameUtil;
import com.freiheit.fuava.sftp.util.FileType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for FilenameUtil.
 *
 * @author florian.diebold@freiheit.com
 */
public class FilenameUtilTest {
    /**
     * Test for matchesSearchedFile.
     */
    @Test( dataProvider = "fileTypes" )
    public void testMatchesSearchedFile( final FileType fileType, final String fileName, final boolean shouldMatch ) {
        Assert.assertEquals( FilenameUtil.matchesSearchedFile( fileName, fileType, null, null ), shouldMatch );
    }

    @DataProvider( name = "fileTypes" )
    public Object[][] getFileTypesData() {
        final FileType pwhg = new FileType( "pwhg", "_ho_pwhg_dat{1}" );
        final FileType mcrm = new FileType( "abc", "ABC_ABC_Data" );
        return new Object[][] {
                { pwhg, "de_dev_ho_pwhg_data_20151122_120000.csv", true },
                { pwhg, "de_dev_ho_pwhg_data_0-2_20151121_120000.csv", true },
                { pwhg, "fr_pp_ho_pwhg_data_0-2_20181231_163759.csv", true },
                { pwhg, "de_dev_st_art_dat_15_all_data_0-2_20151121_120000.csv", false },
                { mcrm, "ABC_ABC_Data_152000_20150812_100000.csv", true },
        };
    }
}

