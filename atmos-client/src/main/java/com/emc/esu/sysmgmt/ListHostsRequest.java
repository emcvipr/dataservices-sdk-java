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
public class ListHostsRequest extends SysMgmtRequest<ListHostsResponse> {

	private String rmgName;

	public ListHostsRequest(SysMgmtApi api, String rmgName) {
		super(api);
		this.rmgName = rmgName;
	}

	@Override
	public ListHostsResponse call() {
		try {
			HttpURLConnection con = getConnection("/sysmgmt/rmgs/" + rmgName + "/nodes", null);
			
			con.connect();
			
			int code = con.getResponseCode();
			if(code != 200) {
				handleError(con);
			}

			return new ListHostsResponse(con);
		} catch (IOException e) {
			throw new EsuException("Error connecting to server: " + e.getMessage(), e);
		} catch (URISyntaxException e) {
			throw new EsuException("Error building URI: " + e.getMessage(), e);
		} catch (JDOMException e) {
			throw new EsuException("Error parsing XML response", e);
		}
	}

}
