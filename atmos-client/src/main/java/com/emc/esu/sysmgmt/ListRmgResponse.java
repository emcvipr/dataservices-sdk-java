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

public class ListRmgResponse extends SysMgmtResponse {
	private List<Rmg> rmgs;

	public ListRmgResponse(HttpURLConnection con) throws IOException, JDOMException {
		super(con);
		
		// Parse response
		Document doc = SysMgmtUtils.parseResponseXml(con);
		
		Element root = doc.getRootElement(); //rmgList
		
		rmgs = new ArrayList<ListRmgResponse.Rmg>();
		
		List<?> rmgsXml = root.getChildren("rmg");
		for(Object o : rmgsXml) {
			if(!(o instanceof Element)) {
				throw new EsuException("Expected XML Element got " + o.getClass());
			}
			
			Element e = (Element)o;
			
			Rmg r = new Rmg();
			r.setName(e.getChildText("name"));
			r.setLocalTime(e.getChildText("localTime"));
			r.setNodesUp(Integer.parseInt(e.getChildText("nodesUp")));
			r.setNodesDown(Integer.parseInt(e.getChildText("nodesDown")));
			r.setAvgLoad1(Float.parseFloat(e.getChildText("avgLoad1")));
			r.setAvgLoad5(Float.parseFloat(e.getChildText("avgLoad5")));
			r.setAvgLoad15(Float.parseFloat(e.getChildText("avgLoad15")));
			rmgs.add(r);
		}
	}
	
	
	/**
	 * @return the rmgs
	 */
	public List<Rmg> getRmgs() {
		return rmgs;
	}


	public class Rmg {
		private String name;
		private String localTime;
		private int nodesUp;
		private int nodesDown;
		private float avgLoad1;
		private float avgLoad5;
		private float avgLoad15;
		
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
		 * @return the localTime
		 */
		public String getLocalTime() {
			return localTime;
		}
		/**
		 * @param localTime the localTime to set
		 */
		public void setLocalTime(String localTime) {
			this.localTime = localTime;
		}
		/**
		 * @return the nodesUp
		 */
		public int getNodesUp() {
			return nodesUp;
		}
		/**
		 * @param nodesUp the nodesUp to set
		 */
		public void setNodesUp(int nodesUp) {
			this.nodesUp = nodesUp;
		}
		/**
		 * @return the nodesDown
		 */
		public int getNodesDown() {
			return nodesDown;
		}
		/**
		 * @param nodesDown the nodesDown to set
		 */
		public void setNodesDown(int nodesDown) {
			this.nodesDown = nodesDown;
		}
		/**
		 * @return the avgLoad1
		 */
		public float getAvgLoad1() {
			return avgLoad1;
		}
		/**
		 * @param avgLoad1 the avgLoad1 to set
		 */
		public void setAvgLoad1(float avgLoad1) {
			this.avgLoad1 = avgLoad1;
		}
		/**
		 * @return the avgLoad5
		 */
		public float getAvgLoad5() {
			return avgLoad5;
		}
		/**
		 * @param avgLoad5 the avgLoad5 to set
		 */
		public void setAvgLoad5(float avgLoad5) {
			this.avgLoad5 = avgLoad5;
		}
		/**
		 * @return the avgLoad15
		 */
		public float getAvgLoad15() {
			return avgLoad15;
		}
		/**
		 * @param avgLoad15 the avgLoad15 to set
		 */
		public void setAvgLoad15(float avgLoad15) {
			this.avgLoad15 = avgLoad15;
		}
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "Rmg [name=" + name + ", localTime=" + localTime
					+ ", nodesUp=" + nodesUp + ", nodesDown=" + nodesDown
					+ ", avgLoad1=" + avgLoad1 + ", avgLoad5=" + avgLoad5
					+ ", avgLoad15=" + avgLoad15 + "]";
		}
		
		
	}

}
