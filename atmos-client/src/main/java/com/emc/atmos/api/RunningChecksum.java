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

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Used to store, update and compute checksums
 */
public class RunningChecksum extends ChecksumValue {
    private static final Logger l4j = Logger.getLogger( RunningChecksum.class );

    private ChecksumAlgorithm algorithm;
    private long offset;
    private MessageDigest digest;

    public RunningChecksum( ChecksumAlgorithm algorithm ) throws NoSuchAlgorithmException {
        this.algorithm = algorithm;
        this.offset = 0;
        this.digest = MessageDigest.getInstance( algorithm.getDigestName() );
    }

    /**
     * Updates the checksum with the given buffer's contents
     *
     * @param buffer data to update
     * @param offset start in buffer
     * @param length number of bytes to use from buffer starting at offset
     */
    public void update( byte[] buffer, int offset, int length ) {
        this.digest.update( buffer, offset, length );
        this.offset += length;
    }

    /**
     * Convenience method to pass in a buffer segment.
     */
    public void update( BufferSegment segment ) {
        this.update( segment.getBuffer(), segment.getOffset(), segment.getSize() );
    }

    @Override
    public ChecksumAlgorithm getAlgorithm() {
        return algorithm;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public String getValue() {

        // Clone the digest so we can pad current value for output
        MessageDigest tmpDigest;
        try {
            tmpDigest = (MessageDigest) digest.clone();
        } catch ( CloneNotSupportedException e ) {
            throw new RuntimeException( "Clone failed", e );
        }

        byte[] currDigest = tmpDigest.digest();

        // convert to hex string
        BigInteger bigInt = new BigInteger( 1, currDigest );
        return String.format( "%0" + (currDigest.length << 1) + "x", bigInt );
    }
}