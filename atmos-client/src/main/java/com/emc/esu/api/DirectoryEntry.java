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

/**
 * Directory entries are returned when you list the contents
 * of a directory.
 */
public class DirectoryEntry {
	private ObjectPath path;
	private ObjectId id;
	private String type;
	private MetadataList systemMetadata;
	private MetadataList userMetadata;
	
	/**
	 * @return the path
	 */
	public ObjectPath getPath() {
		return path;
	}
	/**
	 * @param path the path to set
	 */
	public void setPath(ObjectPath path) {
		this.path = path;
	}
	/**
	 * @return the id
	 */
	public ObjectId getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(ObjectId id) {
		this.id = id;
	}
	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}
	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		// TODO Auto-generated method stub
		return path + " - " + type + " - " + id;
	}
	/**
	 * @param systemMetadata the systemMetadata to set
	 */
	public void setSystemMetadata(MetadataList systemMetadata) {
		this.systemMetadata = systemMetadata;
	}
	/**
	 * Gets the system metadata for this directory entry. If metadata was not
	 * requested in the listDirectory method, this will be null.
	 * @return the systemMetadata
	 */
	public MetadataList getSystemMetadata() {
		return systemMetadata;
	}
	/**
	 * @param userMetadata the userMetadata to set
	 */
	public void setUserMetadata(MetadataList userMetadata) {
		this.userMetadata = userMetadata;
	}
	/**
	 * Gets the user metadata for this directory entry. If metadata was not
	 * requested in the listDirectory method, this will be null.
	 * @return the userMetadata
	 */
	public MetadataList getUserMetadata() {
		return userMetadata;
	}
	
}
