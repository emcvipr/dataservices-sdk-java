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
package com.emc.esu.sysmgmt.pox;

import java.net.HttpURLConnection;
import java.text.MessageFormat;

import com.emc.esu.sysmgmt.SysMgmtApi;

/**
 * @author cwikj
 * 
 */
public class GetUidRequest extends PoxRequest<GetUidResponse> {

	private String subTenantName;
	private String uid;

	public GetUidRequest(SysMgmtApi api) {
		super(api);
	}

	@Override
	public GetUidResponse call() throws Exception {
		HttpURLConnection con = getConnection("/sub_tenant_admin/get_uid",
				MessageFormat.format("app_name={0}&sub_tenant_name={1}", uid,
						subTenantName));

		con.connect();

		return new GetUidResponse(con);
	}

	/**
	 * @return the subTenantName
	 */
	public String getSubTenantName() {
		return subTenantName;
	}

	/**
	 * @param subTenantName the subTenantName to set
	 */
	public void setSubTenantName(String subTenantName) {
		this.subTenantName = subTenantName;
	}

	/**
	 * @return the uid
	 */
	public String getUid() {
		return uid;
	}

	/**
	 * @param uid the uid to set
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}
}
