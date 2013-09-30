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
import java.util.ArrayList;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.emc.esu.api.EsuException;

/**
 * @author cwikj
 *
 */
public class ListHostsResponse extends SysMgmtResponse {
	private List<Host> hosts;

	public ListHostsResponse(HttpURLConnection con) throws IOException, JDOMException {
		super(con);
		
		// Parse response
		Document doc = SysMgmtUtils.parseResponseXml(con);
		
		Element root = doc.getRootElement(); //rmgList
		
		hosts = new ArrayList<ListHostsResponse.Host>();
		
		List<?> hostsXml = root.getChildren("node");
		for(Object o : hostsXml) {
			if(!(o instanceof Element)) {
				throw new EsuException("Expected XML Element got " + o.getClass());
			}
			
			Element e = (Element)o;
			
			Host h = new Host();
			
			h.setName(e.getAttributeValue("name"));
			h.setUp(Boolean.parseBoolean(e.getAttributeValue("up")));
			h.setLocation(e.getAttributeValue("location"));
			
			hosts.add(h);
		}

	}
	
	/**
	 * @return the hosts
	 */
	public List<Host> getHosts() {
		return hosts;
	}

	public class Host {
		private String name;
		private boolean up;
		private String location;
		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}
		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}
		/**
		 * @return the up
		 */
		public boolean isUp() {
			return up;
		}
		/**
		 * @param up the up to set
		 */
		public void setUp(boolean up) {
			this.up = up;
		}
		/**
		 * @return the location
		 */
		public String getLocation() {
			return location;
		}
		/**
		 * @param location the location to set
		 */
		public void setLocation(String location) {
			this.location = location;
		}
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "Host [name=" + name + ", up=" + up + ", location="
					+ location + "]";
		}
		
	}

}
