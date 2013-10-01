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
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 * @author cwikj
 *
 */
public class SysMgmtUtils {
	private static final Logger l4j = Logger.getLogger(SysMgmtUtils.class);
	
    /**
     * Reads the response body and returns it in a byte array.
     * 
     * @param con the HTTP connection
     * @return the byte array containing the response body. Note that if you
     *         pass in a buffer, this will the same buffer object. Be sure to
     *         check the content length to know what data in the buffer is valid
     *         (from zero to contentLength).
     * @throws IOException if reading the response stream fails.
     */
    public static byte[] readResponse(HttpURLConnection con)
            throws IOException {
        InputStream in = null;
        if (con.getResponseCode() > 299) {
            in = con.getErrorStream();
            if (in == null) {
                in = con.getInputStream();
            }
        } else {
            in = con.getInputStream();
        }
        if (in == null) {
            // could not get stream
            return new byte[0];
        }
        try {
            byte[] output;
            int contentLength = con.getContentLength();
            // If we know the content length, read it directly into a buffer.
            if (contentLength != -1) {
                output = new byte[con.getContentLength()];

                int c = 0;
                while (c < contentLength) {
                    int read = in.read(output, c, contentLength - c);
                    if (read == -1) {
                        // EOF!
                        throw new EOFException(
                                "EOF reading response at position " + c
                                        + " size " + (contentLength - c));
                    }
                    c += read;
                }

                return output;
            } else {
                l4j.debug("Content length is unknown.  Buffering output.");
                // Else, use a ByteArrayOutputStream to collect the response.
                byte[] buffer = new byte[4096];

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int c = 0;
                while ((c = in.read(buffer)) != -1) {
                    baos.write(buffer, 0, c);
                }
                baos.close();

                l4j.debug("Buffered " + baos.size() + " response bytes");

                return baos.toByteArray();
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
    
	public static Document parseResponseXml(HttpURLConnection con) throws IOException, JDOMException {
		byte[] data = readResponse(con);
		if(l4j.isDebugEnabled()) {
			l4j.debug("Response: " + new String(data, "UTF-8"));
		}
		
        SAXBuilder sb = new SAXBuilder();

        Document d = sb.build(new ByteArrayInputStream(data));

        return d;
	}



}
