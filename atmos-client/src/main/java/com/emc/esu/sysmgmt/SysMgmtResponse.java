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

import java.net.HttpURLConnection;
import java.util.Date;

/**
 * Base class for system management responses.
 * @author cwikj
 *
 */
public class SysMgmtResponse {
	private static final String ATMOS_SYS_MGMT_VERSION = "x-atmos-sysmgmt-version";
	private static final String ATMOS_SYS_MGMT_VERSION_TYPO1 = "x-atoms-sysmgmt-version";
	private static final String ATMOS_SYS_MGMT_VERSION_TYPO2 = "x-atoms-sysmgnt-version";

	private String atmosSysMgmgtVersion;
	private Date serverDate;

	public SysMgmtResponse(HttpURLConnection response) {
		this.atmosSysMgmgtVersion = response.getHeaderField(ATMOS_SYS_MGMT_VERSION);
		if(this.atmosSysMgmgtVersion == null) {
			this.atmosSysMgmgtVersion = response.getHeaderField(ATMOS_SYS_MGMT_VERSION_TYPO1);
		}
		if(this.atmosSysMgmgtVersion == null) {
			this.atmosSysMgmgtVersion = response.getHeaderField(ATMOS_SYS_MGMT_VERSION_TYPO2);
		}
		this.serverDate = new Date(response.getDate());
	}
	
	public Date getServerDate() {
		return serverDate;
	}
	
	public String getAtmosSysMgmtVersion() {
		return atmosSysMgmgtVersion;
	}

}
