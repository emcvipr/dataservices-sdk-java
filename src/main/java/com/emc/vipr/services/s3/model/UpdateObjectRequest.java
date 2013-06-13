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
	 * Gets the current update range as a string.
	 * @return the updateRange
	 */
	public String getUpdateRange() {
		return updateRange;
	}

	/**
	 * Explicitly sets the update range as a string value.  No checking is done for 
	 * correctness.  The "bytes=" portion of the header should be omitted.
	 * @param updateRange the updateRange to set (e.g. "100-199").  See RFC 2616, section
	 * 14.35.1 for more information on byte ranges.
	 */
	public void setUpdateRange(String updateRange) {
		this.updateRange = updateRange;
	}
	
	/**
	 * Sets the update range in the object.
	 * 
	 * @param begin the beginning byte offset to update.
	 * @param end the ending byte offset, <em>inclusive</em>.
	 * @return the current request for builder chaining.
	 */
	public UpdateObjectRequest withUpdateRange(long begin, long end) {
		setUpdateRange(begin + "-" + end);
		return this;
	}
	
	/**
	 * Sets the update start position within the object.  The ending position will be
	 * determined by the Content-Length of the request.  If the given startOffset is
	 * past the end of the object, an error will be returned when the request is executed.
	 * 
	 * @param startOffset the start offset position within the object.
     * @return the current request for builder chaining.
	 */
	public UpdateObjectRequest withUpdateOffset(long startOffset) {
		setUpdateRange(startOffset + "-");
		return this;
	}

}
