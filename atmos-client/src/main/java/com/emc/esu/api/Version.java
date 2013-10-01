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
package com.emc.esu.api;

import java.util.Date;

/**
 * Contains the results from the listVersions command.
 * @author cwikj
 */
public class Version {
	private ObjectId id;
	private int versionNumber;
	private Date itime;
	
	public Version(ObjectId id, int versionNumber, Date itime) {
		this.id = id;
		this.versionNumber = versionNumber;
		this.itime = itime;
	}
	
	/**
	 * @return the id
	 */
	public ObjectId getId() {
		return id;
	}
	/**
	 * @return the versionNumber
	 */
	public int getVersionNumber() {
		return versionNumber;
	}
	/**
	 * @return the itime
	 */
	public Date getItime() {
		return itime;
	}
	
	@Override
	public boolean equals(Object obj) {
		Version other = (Version)obj;
		return other.getId().equals(id) && other.getVersionNumber() == versionNumber;
	}
	
	@Override
	public int hashCode() {
		return id.toString().hashCode();
	}
}
