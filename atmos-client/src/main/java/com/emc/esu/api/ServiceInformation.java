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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains information from the GetServiceInformation call
 * @author jason
 */
public class ServiceInformation {
	public static final String FEATURE_OBJECT = "object";
	public static final String FEATURE_NAMESPACE = "namespace";
	public static final String FEATURE_UTF_8 = "utf-8";
	public static final String BROWSER_COMPAT = "browser-compat";
	public static final String KEY_VALUE = "key-value";
	public static final String HARDLINK = "hardlink";
	public static final String QUERY = "query";
	public static final String VERSIONING = "versioning";
	
	private String atmosVersion;
	private boolean unicodeMetadataSupported = false;
	private Set<String> features;
	
	public ServiceInformation() {
		features = new HashSet<String>();
	}
	
	/**
	 * Adds a feature to the list of supported features.
	 */
	public void addFeature(String feature) {
		features.add(feature);
	}
	
	/**
	 * Checks to see if a feature is supported.
	 */
	public boolean hasFeature(String feature) {
		return features.contains(feature);
	}
	
	/**
	 * Gets the features advertised by the service
	 */
	public Set<String> getFeatures() {
		return Collections.unmodifiableSet(features);
	}

	/**
	 * @return the atmosVersion
	 */
	public String getAtmosVersion() {
		return atmosVersion;
	}

	/**
	 * @param atmosVersion the atmosVersion to set
	 */
	public void setAtmosVersion(String atmosVersion) {
		this.atmosVersion = atmosVersion;
	}

	public boolean isUnicodeMetadataSupported() {
		return unicodeMetadataSupported;
	}

	public void setUnicodeMetadataSupported(boolean unicodeMetadataSupported) {
		this.unicodeMetadataSupported = unicodeMetadataSupported;
	}

	
}
