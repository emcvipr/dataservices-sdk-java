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
package com.emc.atmos.sync.monitor;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

public class DirectoryHash {
    private static final String HASH_SEPARATOR = "|";

    private String hash = "";

    /**
     * updates the hash with the latest state of the directory
     *
     * @param directory the directory from which to update the hash
     * @return true if the hash has changed (the directory or its contents have changed), false if it's the same
     */
    public boolean update( File directory ) {
        if ( !directory.exists() )
            throw new IllegalArgumentException( directory.getAbsolutePath() + " does not exist" );
        if ( !directory.isDirectory() )
            throw new IllegalArgumentException( directory.getAbsolutePath() + " is not a directory" );
        if ( !directory.canRead() )
            throw new IllegalArgumentException( directory.getAbsolutePath() + " is not readable" );

        String hash = generateHash( directory );

        if ( !this.hash.equals( hash ) ) {
            this.hash = hash;
            return true;
        }
        return false;
    }

    /**
     * generates a hash of the specified file/directory
     *
     * @param file a file for which to generate a hash
     * @return the hash of the specified file (including all of its children if it's a directory)
     */
    private String generateHash( File file ) {
        String hash = file.getAbsolutePath() + HASH_SEPARATOR + file.length() + HASH_SEPARATOR + file.lastModified() + "\n";
        if ( file.isDirectory() ) {
            File[] list = file.listFiles();

            // sort alphabetically (String.compareTo)
            Arrays.sort( list, new Comparator<File>() {
                @Override
                public int compare( File fileA, File fileB ) {
                    return fileA.getName().compareTo( fileB.getName() );
                }
            } );
            for ( File child : list ) {
                hash += generateHash( child );
            }
        }
        return hash;
    }
}
