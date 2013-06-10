package com.emc.vipr.services.s3.model;

import java.io.File;
import java.io.InputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class UpdateObjectRequest extends PutObjectRequest {
	private String updateRange;

	public UpdateObjectRequest(String bucketName, String key, File file) {
		super(bucketName, key, file);
	}

	public UpdateObjectRequest(String bucketName, String key,
			InputStream input, ObjectMetadata metadata) {
		super(bucketName, key, input, metadata);
	}

	public UpdateObjectRequest(String bucketName, String key,
			String redirectLocation) {
		super(bucketName, key, redirectLocation);
	}

	/**
	 * @return the updateRange
	 */
	public String getUpdateRange() {
		return updateRange;
	}

	/**
	 * @param updateRange the updateRange to set
	 */
	public void setUpdateRange(String updateRange) {
		this.updateRange = updateRange;
	}
	
	public UpdateObjectRequest withUpdateRange(long begin, long end) {
		
		return this;
	}
	
	public UpdateObjectRequest withUpdateOffset(long startOffset) {
		
		return this;
	}

}
