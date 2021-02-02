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
package com.freiheit.fuava.sftp.testclient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TestFolder<T> {
    final ConcurrentMap<String, T> folderContent;

    public TestFolder( final Map<String, T> folderContent ) {
        this.folderContent = new ConcurrentHashMap<>( folderContent );
    }

    public Set<String> getItemKeys() {
        return folderContent.keySet();
    }

    public T getItem( final String name ) {
        return folderContent.get( name );
    }

    public void removeItem( final String fileName ) {
        folderContent.remove( fileName );
    }

    public void addItem( final String fileName, final T item ) {
        folderContent.put( fileName, item );
    }
}