package com.freiheit.fuava.sftp.testclient;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class TestFolder<T> {
    final Map<String, T> folderContent;

    public TestFolder( final Map<String, T> folderContent ) {
        this.folderContent = new HashMap<String, T>( folderContent );
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