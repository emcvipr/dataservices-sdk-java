package com.emc.vipr.services.s3.model;

import java.io.File;
import java.io.InputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;

public class AppendObjectRequest extends UpdateObjectRequest {
	private static final String UPDATE_RANGE = "-1-";

	public AppendObjectRequest(String bucketName, String key, File file) {
		super(bucketName, key, file);
		
		setUpdateRange(UPDATE_RANGE);
	}

	public AppendObjectRequest(String bucketName, String key,
			InputStream input, ObjectMetadata metadata) {
		super(bucketName, key, input, metadata);
		
		setUpdateRange(UPDATE_RANGE);
	}

	public AppendObjectRequest(String bucketName, String key,
			String redirectLocation) {
		super(bucketName, key, redirectLocation);
		
		setUpdateRange(UPDATE_RANGE);
	}

}
