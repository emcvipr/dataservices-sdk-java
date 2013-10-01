/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.atmos.api;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.bean.DirectoryEntry;

/**
 * Represents the full path to an object within a subtenant namespace. Paths always start with a slash. Directories
 * always end with a slash and non-directories never end with a slash.
 */
public class ObjectPath implements ObjectIdentifier {
    private String path;

    public ObjectPath( String path ) {
        this.path = cleanPath( path );
    }

    /**
     * Constructs a new path underneath a parent directory.
     *
     * @param parent The parent directory under which this new path exists (must end with a slash).
     * @param path   The relative path to this object under the parent directory (may begin with a slash, but it will
     *               be ignored).
     */
    public ObjectPath( ObjectPath parent, String path ) {
        if ( !parent.isDirectory() ) throw new AtmosException( "parent path must be a directory (end with a slash)" );
        // remove trailing slash from parent
        String parentPath = parent.getPath();
        this.path = parentPath.substring( 0, parentPath.length() - 1 ) + cleanPath( path );
    }

    /**
     * Constructs a new path from a parent directory and (what is assumed to be) one of its directory entries. The
     * resulting path will be a directory or file consistent with the directoryEntry.
     *
     * @param parent         The parent directory under which this new path exists (must end with a slash).
     * @param directoryEntry A directory entry of an object (presumed to be under the parent path)
     */
    public ObjectPath( ObjectPath parent, DirectoryEntry directoryEntry ) {
        this( parent, directoryEntry.getFilename() + (directoryEntry.isDirectory() ? "/" : "") );
    }

    public String getPath() {
        return this.path;
    }

    @Override
    public String getRelativeResourcePath() {
        return "namespace" + path;
    }

    @Override
    public String toString() {
        return getPath();
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        ObjectPath that = (ObjectPath) o;

        if ( !path.equals( that.path ) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    /**
     * Convenience method to determine whether this path represents a directory in Atmos.  Atmos uses a convention
     * where directory paths always end with a slash and object paths do not.
     */
    public boolean isDirectory() {
        return path.charAt( path.length() - 1 ) == '/';
    }

    /**
     * Convenience method to return the filename of this path (the last token delimited by a slash)
     */
    public String getFilename() {
        String[] levels = path.split( "/" );
        if ( levels[levels.length - 1].length() == 0 )
            return levels[levels.length - 2];
        else
            return levels[levels.length - 1];
    }

    private String cleanPath( String path ) {
        // require beginning slash
        if ( path.charAt( 0 ) != '/' ) path = '/' + path;
        return path;
    }
}
