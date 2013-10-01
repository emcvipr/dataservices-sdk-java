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

import java.io.IOException;
import java.net.HttpURLConnection;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.emc.esu.sysmgmt.SysMgmtUtils;

/**
 * @author cwikj
 *
 */
public class GetUidResponse extends PoxResponse {

	public GetUidResponse(HttpURLConnection con) throws IOException, JDOMException {
		// Parse response
		Document doc = SysMgmtUtils.parseResponseXml(con);
		
		Element root = doc.getRootElement(); //rmgList
	}

}
