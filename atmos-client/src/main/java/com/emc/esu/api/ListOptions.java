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

import java.util.List;

/**
 * Allows you to specify extended options when listing directories or listing
 * objects.  When using paged directory responses (limit > 0), the token
 * used for subsequent responses will be returned through this object.
 * @since 1.4.1
 */
public class ListOptions {
	private int limit;
	private String token;
	private List<String> userMetadata;
	private List<String> systemMetadata;
	private boolean includeMetadata;
	
	/**
	 * Returns the current results limit.  Zero indicates all results.
	 * 
	 * @return the limit
	 */
	public int getLimit() {
		return limit;
	}
	
	/**
	 * Sets the maximum number of results to fetch.  Set to zero to fetch all
	 * remaining results.
	 */
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	/**
	 * Returns the token used to request more results.  If no more results
	 * are available, the token will be null.
	 * 
	 * @return the token
	 */
	public String getToken() {
		return token;
	}
	
	/**
	 * Sets the token to request more results.  Normally, this will only
	 * be called internally by the API.
	 * 
	 * @param token the token to set
	 */
	public void setToken(String token) {
		this.token = token;
	}
	
	/**
	 * Returns true if metadata is included in the response.
	 * @return the includeMetadata
	 */
	public boolean isIncludeMetadata() {
		return includeMetadata;
	}
	
	/**
	 * Set to true if you want object metadata included in the response
	 * @param includeMetadata the includeMetadata to set
	 */
	public void setIncludeMetadata(boolean includeMetadata) {
		this.includeMetadata = includeMetadata;
	}

	/**
	 * When includeMetadata is true, returns the requested set of user metadata
	 * values to include in the result.  A null list requests all 
	 * results.
	 * @return the userMetadata requested
	 */
	public List<String> getUserMetadata() {
		return userMetadata;
	}

	/**
	 * When includeMetadata is true, sets the list of user metadata values to 
	 * include in the results.  Set to null to request all metadata.	 
	 * @param userMetadata the userMetadata to set
	 */
	public void setUserMetadata(List<String> userMetadata) {
		this.userMetadata = userMetadata;
	}

	/**
	 * When includeMetadata is true, returns the requested set of user metadata
	 * values to include in the result.  A null list requests all 
	 * results.
	 * @return the systemMetadata requested
	 */
	public List<String> getSystemMetadata() {
		return systemMetadata;
	}

	/**
	 * When includeMetadata is true, sets the list of system metadata values to 
	 * include in the results.  Set to null to request all metadata.	 
	 * @param systemMetadata the systemMetadata to set
	 */
	public void setSystemMetadata(List<String> systemMetadata) {
		this.systemMetadata = systemMetadata;
	}
	
}
