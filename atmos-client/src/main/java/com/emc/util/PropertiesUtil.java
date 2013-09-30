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
package com.emc.util;

import java.io.*;
import java.util.Set;
import java.util.TreeSet;

public class PropertiesUtil {
    private static Set<String> loadedFiles = new TreeSet<String>();

    private static void loadConfig( String fileName ) {
        try {
            InputStream in = ClassLoader.getSystemResourceAsStream( fileName );

            // try in home directory too
            if ( in == null )
                in = new FileInputStream( new File( System.getProperty( "user.home" ) + File.separator + fileName ) );

            System.getProperties().load( in );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not load " + fileName, e );
        }
    }

    public static String getRequiredProperty( String fileName, String key ) {
        String value = getProperty(fileName, key);
        if ( value == null )
            throw new RuntimeException( key + " is null.  Set in " + fileName + " or on command line with -D" + key );
        return value;
    }

    public static synchronized String getProperty( String fileName, String key ) {
        if ( !loadedFiles.contains( fileName ) ) {
            loadConfig( fileName );
            loadedFiles.add( fileName );
        }

        return System.getProperty( key );
    }

    private PropertiesUtil() {
    }
}
