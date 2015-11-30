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
package com.freiheit.fuava.simplebatch.processor;

final class ControlFilePersistenceConfigImpl implements
        ControlFilePersistence.Configuration {
    private final String dirName;
    private final String controlFileEnding;
    private final String logFileEnding;

    ControlFilePersistenceConfigImpl(
            final String dirName,
            final String controlFileEnding,
            final String logFileEnding ) {
        this.dirName = dirName;
        this.controlFileEnding = controlFileEnding;
        this.logFileEnding = logFileEnding;
    }

    @Override
    public String getDownloadDirPath() {
        return dirName;
    }

    @Override
    public String getControlFileEnding() {
        return controlFileEnding;
    }

    @Override
    public String getLogFileEnding() {
        return logFileEnding;
    }

}