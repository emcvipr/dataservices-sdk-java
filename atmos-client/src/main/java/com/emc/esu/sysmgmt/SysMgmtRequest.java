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
package com.emc.esu.sysmgmt;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.emc.esu.api.EsuException;

/**
 * @author cwikj
 *
 */
public abstract class SysMgmtRequest<T extends SysMgmtResponse> implements Callable<T> {
	private static final Logger l4j = Logger.getLogger(SysMgmtRequest.class);

	private static final String PROP_USERNAME = "x-atmos-systemadmin";
	private static final String PROP_PASSWORD = "x-atmos-systemadminpassword";
	private static final String PASSWORD_AUTH = "password";
	private static final String PROP_AUTHTYPE = "x-atmos-authtype";
	
	
	protected SysMgmtApi api;

	public SysMgmtRequest(SysMgmtApi api) {
		this.api = api;
	}

	protected HttpURLConnection getConnection(String path, String query) 
			throws IOException, URISyntaxException {
		
        URI uri = new URI( api.getProto(), null, api.getHost(), 
        		api.getPort(), path, query, null );
        l4j.debug("URI: " + uri);
        URL u = new URL(uri.toASCIIString());
        l4j.debug( "URL: " + u );

        HttpURLConnection con = (HttpURLConnection) u.openConnection();
        
        con.addRequestProperty(PROP_USERNAME, api.getUsername());
        con.addRequestProperty(PROP_PASSWORD, api.getPassword());
        con.addRequestProperty(PROP_AUTHTYPE, PASSWORD_AUTH);
        
        return con;
	}
	
	protected void handleError(HttpURLConnection con) throws IOException, JDOMException {
		int httpCode = con.getResponseCode();
		String msg = con.getResponseMessage();
		
        byte[] response = SysMgmtUtils.readResponse(con);
        l4j.debug("Error response: " + new String(response, "UTF-8"));
        SAXBuilder sb = new SAXBuilder();

        Document d = sb.build(new ByteArrayInputStream(response));

        String code = d.getRootElement().getChildText("Code");
        String message = d.getRootElement().getChildText("Message");

        if (code == null && message == null) {
            // not an error from ESU
            throw new EsuException(msg, httpCode);
        }

        l4j.debug("Error: " + code + " message: " + message);
        throw new EsuException(message, httpCode, Integer.parseInt(code));
	}

    


}
