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
 * This object encapsulates all of the information about an Atmos
 * object when returned from getAllMetadata (the HEAD request version)
 * @author jason
 *
 */
public class ObjectMetadata {
	private MetadataList metadata;
	private Acl acl;
	private String mimeType;
	
	/**
	 * @param metadata the metadata to set
	 */
	public void setMetadata(MetadataList metadata) {
		this.metadata = metadata;
	}
	/**
	 * @return the metadata
	 */
	public MetadataList getMetadata() {
		return metadata;
	}
	/**
	 * @param acl the acl to set
	 */
	public void setAcl(Acl acl) {
		this.acl = acl;
	}
	/**
	 * @return the acl
	 */
	public Acl getAcl() {
		return acl;
	}
	/**
	 * @return the mimeType
	 */
	public String getMimeType() {
		return mimeType;
	}
	/**
	 * @param mimeType the mimeType to set
	 */
	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}
}
