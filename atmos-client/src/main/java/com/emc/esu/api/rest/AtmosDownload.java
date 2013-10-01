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
package com.emc.esu.api.rest;

import java.io.File;

import com.emc.esu.api.EsuException;
import com.emc.esu.api.Identifier;
import com.emc.esu.api.ObjectId;
import com.emc.esu.api.ObjectPath;

public class AtmosDownload {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String atmosHost = args[0];
		int port = Integer.parseInt( args[1] );
		String atmosUid = args[2];
		String atmosSecret = args[3];
		String atmosPath = args[4];
		File localFile = new File(args[5]);
		
		EsuRestApi esu = new EsuRestApi( atmosHost, port, atmosUid, atmosSecret);
		
		System.err.println( "Downloading " + atmosPath + " to " + localFile );
		
		Identifier id = null;
		if( atmosPath.contains( "/" ) ) {
			id = new ObjectPath( atmosPath );
		} else {
			id = new ObjectId( atmosPath );
		}
		
		try {
			DownloadHelper dh = new DownloadHelper( esu, new byte[5000] );
			dh.readObject( id,localFile );
		} catch( EsuException e ) {
			System.err.println( "Download Failed" );
			e.printStackTrace();
		}
	}

}
