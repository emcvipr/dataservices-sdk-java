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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.apache.log4j.Logger;

public class LBEsuRestApiApache extends EsuRestApiApache {
	private static final Logger l4j = Logger.getLogger(LBEsuRestApiApache.class);
	
	public static enum LBMode { ROUND_ROBIN, ROUND_ROBIN_THREADS };
	
	private List<String> hosts;
	private long requestCount = 0L;
	private LBMode mode = LBMode.ROUND_ROBIN_THREADS;
	private ThreadLocal<String> threadHost = new ThreadLocal<String>();
	
    public LBEsuRestApiApache(List<String> hosts, int port, String uid, String sharedSecret) {
        super(hosts.get(0), port, uid, sharedSecret);
    	this.hosts = hosts;
    }

	@Override
	protected URL buildUrl(String resource, String query) throws URISyntaxException, MalformedURLException {
		
		int uriport =0;
		if( "http".equals(proto) && port == 80 ) {
			// Default port
			uriport = -1;
		} else if( "https".equals(proto) && port == 443 ) {
			uriport = -1;
		} else {
			uriport = port;
		}
		
		String host = null;
		
		if( mode == LBMode.ROUND_ROBIN_THREADS ) {
			// Bind thread to a specific host
			if( threadHost.get() == null ) {
				threadHost.set( hosts.get( (int)(requestCount++ % hosts.size()) ) );
				l4j.info( "Thread bound to " + threadHost.get() );
			}
			host = threadHost.get();
		} else {
			host = hosts.get( (int)(requestCount++ % hosts.size()) );
		}
		
	    URI uri = new URI( proto, null, host, uriport, resource, query, null );
	    URL u = uri.toURL();
	    l4j.debug( "URL: " + u );
	    return u;
	}

	public LBMode getMode() {
		return mode;
	}

	public void setMode(LBMode mode) {
		this.mode = mode;
	}
}
