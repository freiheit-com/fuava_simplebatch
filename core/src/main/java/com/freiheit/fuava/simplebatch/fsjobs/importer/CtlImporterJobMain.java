/**
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
package com.freiheit.fuava.simplebatch.fsjobs.importer;

import com.freiheit.fuava.simplebatch.BatchJobMain;

/**
 * Helper class for implementing simple command line programs which import data
 * from files on the file system, implementing a control-files based protocol.
 * 
 * @author klas
 *
 */
public class CtlImporterJobMain {

    public static <Output> int exec( final CtlImporterJob<Output> batchJob ) {
        return BatchJobMain.exec( batchJob );
    }

}
