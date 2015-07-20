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

    public static <Output> void exec( final CtlImporterJob<Output> batchJob ) {
        BatchJobMain.exec( batchJob );
    }

}
