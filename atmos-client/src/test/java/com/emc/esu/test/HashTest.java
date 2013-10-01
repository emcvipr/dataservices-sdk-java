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
package com.emc.esu.test;

import com.emc.esu.api.Checksum;
import com.emc.esu.api.Checksum.Algorithm;

public class HashTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Checksum ck = new Checksum(Algorithm.SHA0);
			byte[] hello = "hello".getBytes("US-ASCII");
			byte[] world = " world".getBytes("US-ASCII");
			
			ck.update(hello, 0, hello.length);
			System.out.println( "Hash after 'hello' " + ck );
			
			ck.update(world, 0, world.length);
			System.out.println(  "Hash after ' world'" + ck );
			
			byte[] helloworld = "hello world".getBytes("US-ASCII");
			ck = new Checksum( Algorithm.SHA0 );
			ck.update( helloworld, 0, helloworld.length );
			System.out.println( "full: " + ck );
		} catch( Exception e ) {
			e.printStackTrace();
		}
		
		
	}

}
