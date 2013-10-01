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
import java.util.ArrayList;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.emc.esu.api.EsuException;
import com.emc.esu.sysmgmt.SysMgmtUtils;

/**
 * @author cwikj
 *
 */
public class ListRmgResponsePox extends PoxResponse {
	private List<Rmg> rmgs;
	
	public ListRmgResponsePox(HttpURLConnection con) throws IOException, JDOMException {
		
		// Parse response
		Document doc = SysMgmtUtils.parseResponseXml(con);
		
		Element root = doc.getRootElement(); //rmgList
		
		
		rmgs = new ArrayList<Rmg>();
		
		List<?> rmgsXml = root.getChildren("rmg");
		// Error check
		if(rmgsXml.size() < 1) {
			setSuccessful(false);
			setError(root.getTextTrim());
			return;
		}
		
		for(Object o : rmgsXml) {
			if(!(o instanceof Element)) {
				throw new EsuException("Expected XML Element got " + o.getClass());
			}
			
			Element e = (Element)o;
			
			Rmg r = new Rmg();
			r.setName(e.getChildText("name"));
			r.setId(Integer.parseInt(e.getChildText("id")));
			r.setLocation(e.getChildText("location"));
			r.setCapacity(e.getChildText("capacity"));
			r.setMulticastAddress(e.getChildText("multicast_address"));
			rmgs.add(r);
		}
	}


	public static class Rmg {
		private String name;
		private int id;
		private String location;
		private String capacity;
		private String multicastAddress;
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
		 * @return the id
		 */
		public int getId() {
			return id;
		}
		/**
		 * @param id the id to set
		 */
		public void setId(int id) {
			this.id = id;
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
		/**
		 * @return the capacity
		 */
		public String getCapacity() {
			return capacity;
		}
		/**
		 * @param capacity the capacity to set
		 */
		public void setCapacity(String capacity) {
			this.capacity = capacity;
		}
		/**
		 * @return the multicastAddress
		 */
		public String getMulticastAddress() {
			return multicastAddress;
		}
		/**
		 * @param multicastAddress the multicastAddress to set
		 */
		public void setMulticastAddress(String multicastAddress) {
			this.multicastAddress = multicastAddress;
		}
	}


	/**
	 * @return the rmgs
	 */
	public List<Rmg> getRmgs() {
		return rmgs;
	}
}
