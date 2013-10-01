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
package com.emc.vipr.services.s3.model;

import com.amazonaws.AmazonWebServiceRequest;

public class GetFileAccessRequest extends AmazonWebServiceRequest {
	private String bucketName;
	private String marker;
	private int maxKeys;
	
	/**
	 * @return the bucketName
	 */
	public String getBucketName() {
		return bucketName;
	}
	/**
	 * @param bucketName the bucketName to set
	 */
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	/**
	 * @return the marker
	 */
	public String getMarker() {
		return marker;
	}
	/**
	 * @param marker the marker to set
	 */
	public void setMarker(String marker) {
		this.marker = marker;
	}
	/**
	 * @return the maxKeys
	 */
	public int getMaxKeys() {
		return maxKeys;
	}
	/**
	 * @param maxKeys the maxKeys to set
	 */
	public void setMaxKeys(int maxKeys) {
		this.maxKeys = maxKeys;
	}
	
	
}
