package com.freiheit.fuava.sftp.testclient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TestFolder<T> {
    final ConcurrentMap<String, T> folderContent;

    public TestFolder( final Map<String, T> folderContent ) {
        this.folderContent = new ConcurrentHashMap<String, T>( folderContent );
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