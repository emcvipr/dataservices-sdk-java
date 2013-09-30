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
package com.emc.esu.api;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;

import org.apache.log4j.Logger;
import org.concord.security.ccjce.cryptix.jce.provider.CryptixCrypto;

/**
 * The checksum class is used to store and compute partial checksums when uploading
 * files.
 */
public class Checksum {
	private static final Logger l4j = Logger.getLogger( Checksum.class );
	
	// Register SHA-0 provider
	static {
		Security.addProvider( new CryptixCrypto() );
	}

	/**
	 * The hash algorithm to use.  As of 1.4.0, only SHA0 is supported
	 */
	public static enum Algorithm { SHA0, SHA1, MD5 };

	private MessageDigest digest;
	private Algorithm alg;
	private long offset;
	private String expectedValue;

    public Checksum( Algorithm alg ) throws NoSuchAlgorithmException {
		switch( alg ) {
		case SHA0:
			digest = MessageDigest.getInstance( "SHA-0" );
			break;
		case SHA1:
			digest = MessageDigest.getInstance( "SHA-1" );
			break;
		case MD5:
			digest = MessageDigest.getInstance( "MD5" );
			break;
		}
		this.alg = alg;
		offset = 0;
	}
	
	public String getAlgorithmName() {
		switch( alg ) {
			case SHA0:
				return "SHA0";
			case SHA1:
				return "SHA1";
			case MD5:
				return "MD5";
		}
		throw new RuntimeException( "Unknown algorithm: " + alg );
		
	}

	/**
	 * Updates the checksum with the given buffer's contents
	 * @param buffer data to update
	 * @param offset start in buffer
	 * @param length number of bytes to use from buffer starting at offset
	 */
	public void update( byte[] buffer, int offset, int length ) {
		digest.update(buffer, offset, length);
		this.offset += length;
	}

	/**
	 * Outputs the current digest checksum in a format
	 * suitable for including in Atmos create/update calls.
	 */
	@Override
	public String toString() {
		
		String checksumData = getAlgorithmName()+"/"+offset+"/"+getHashValue();
		l4j.debug( "Checksum Value: '" + checksumData + "'" );
		
		return checksumData;
	}

	private String getHashValue() {
		// Clone the digest so we can pad current value for output
		MessageDigest tmpDigest;
		try {
			tmpDigest = (MessageDigest) digest.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException( "Clone failed", e );
		}
		
		byte[] currDigest = tmpDigest.digest();

        // convert to hex string
		BigInteger bigInt = new BigInteger(1, currDigest);
        return String.format( "%0" + (currDigest.length << 1) + "x", bigInt );
	}
    
	/**
	 * Sets the expected value for this checksum.  Only used for read operations
	 * @param expectedValue the expectedValue to set
	 */
	public void setExpectedValue(String expectedValue) {
		this.expectedValue = expectedValue;
	}

	/**
	 * Gets the expected value for this checksum.  Only used for read operations.
	 * @return the expectedValue
	 */
	public String getExpectedValue() {
		return expectedValue;
	}



}
