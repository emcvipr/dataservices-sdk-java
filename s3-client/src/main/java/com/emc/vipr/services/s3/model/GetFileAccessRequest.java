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
