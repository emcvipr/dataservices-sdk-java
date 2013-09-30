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

import java.util.List;

import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessMode;

public class BucketFileAccessModeResult {
	private FileAccessMode accessMode;
	private long duration;
	private List<String> hostList;
	private String uid;
	private String startToken;
    private String endToken;
	
	/**
	 * @return the accessMode
	 */
	public FileAccessMode getAccessMode() {
		return accessMode;
	}
	/**
	 * @param accessMode the accessMode to set
	 */
	public void setAccessMode(FileAccessMode accessMode) {
		this.accessMode = accessMode;
	}
	/**
	 * @return the duration
	 */
	public long getDuration() {
		return duration;
	}
	/**
	 * @param duration the duration to set
	 */
	public void setDuration(long duration) {
		this.duration = duration;
	}
	/**
	 * @return the hostList
	 */
	public List<String> getHostList() {
		return hostList;
	}
	/**
	 * @param hostList the hostList to set
	 */
	public void setHostList(List<String> hostList) {
		this.hostList = hostList;
	}
	/**
	 * @return the uid
	 */
	public String getUid() {
		return uid;
	}
	/**
	 * @param uid the uid to set
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}
	/**
	 * @return the token that starts this file access window of objects
	 */
	public String getStartToken() {
		return startToken;
	}
	/**
	 * @param startToken the token that starts this file access window of objects
	 */
	public void setStartToken(String startToken) {
		this.startToken = startToken;
	}
    /**
     * @return the token that ends this file access window of objects
     */
    public String getEndToken() {
        return endToken;
    }
    /**
     * @param endToken the token that ends this file access window of objects
     */
    public void setEndToken(String endToken) {
        this.endToken = endToken;
    }
}
