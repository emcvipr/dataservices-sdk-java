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
package com.emc.atmos.api.jersey;

import java.io.IOException;
import java.io.InputStream;

/**
 * A simple delegating class to attach a size to an input stream.  This class does not perform any calculation and
 * merely stores/returns the size specified in the constructor.
 */
public class MeasuredInputStream extends InputStream {
    private InputStream source;
    private long size;
    private long read = 0;

    public MeasuredInputStream( InputStream source, long size ) {
        this.source = source;
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    public long getRead() {
        return read;
    }

    @Override
    public int read() throws IOException {
        int value = source.read();
        if ( value != -1 ) read++;
        return value;
    }

    @Override
    public int read( byte[] bytes ) throws IOException {
        int count = source.read( bytes );
        if ( count != -1 ) read += count;
        return count;
    }

    @Override
    public int read( byte[] bytes, int i, int i1 ) throws IOException {
        int count = source.read( bytes, i, i1 );
        if ( count != -1 ) read += count;
        return count;
    }

    @Override
    public long skip( long l ) throws IOException {
        long count = source.skip( l );
        if ( count != -1 ) read += count;
        return count;
    }

    @Override
    public int available() throws IOException {
        return source.available();
    }

    @Override
    public void close() throws IOException {
        source.close();
    }

    @Override
    public void mark( int i ) {
        source.mark( i );
    }

    @Override
    public void reset() throws IOException {
        source.reset();
    }

    @Override
    public boolean markSupported() {
        return source.markSupported();
    }
}
