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
 * ObjectResults are returned from listObjectsWithMetadata.  They contain the
 * object's ID as well as its MetadataList.
 * @author jason
 *
 */
public class ObjectResult {
	private ObjectId id;
	private MetadataList metadata;
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
	 * @return the metadata
	 */
	public MetadataList getMetadata() {
		return metadata;
	}
	/**
	 * @param metadata the metadata to set
	 */
	public void setMetadata(MetadataList metadata) {
		this.metadata = metadata;
	}
	/**
	 * Equality test.  For convienience, you can compare this to an ObjectId
	 * object to test against the encapuslated ID.
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if( obj instanceof ObjectId ) {
			return id.equals( obj );
		} else {
			return ((ObjectResult)obj).getId().equals( id );
		}
	}
	@Override
	public String toString() {
		return "ObjectResult [id=" + id + ", metadata=" + metadata + "]";
	}
	
	
	
	
}
