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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;

import org.jdom.JDOMException;

import com.emc.esu.api.EsuException;

/**
 * @author cwikj
 *
 */
public class ListRmgRequest extends SysMgmtRequest<ListRmgResponse> {

	public ListRmgRequest(SysMgmtApi atmosSysMgmtApi) {
		super(atmosSysMgmtApi);
	}

	@Override
	public ListRmgResponse call() {
		HttpURLConnection con = null;
		try {
			con = getConnection("/sysmgmt/rmgs", null);
			
			con.connect();
			
			int code = con.getResponseCode();
			if(code != 200) {
				handleError(con);
			}
			
			return new ListRmgResponse(con);
			
		} catch (IOException e) {
			throw new EsuException("Error connecting to server: " + e.getMessage(), e);
		} catch (URISyntaxException e) {
			throw new EsuException("Error building URI: " + e.getMessage(), e);
		} catch (JDOMException e) {
			throw new EsuException("Error parsing XML response", e);
		}

	}
	
}
