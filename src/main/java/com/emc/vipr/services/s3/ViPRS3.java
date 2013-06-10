/**
 * TODO: Copyright here
 */
package com.emc.vipr.services.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.emc.vipr.services.s3.model.*;

import java.io.File;
import java.io.InputStream;

/**
 * @author cwikj
 *
 */
public interface ViPRS3 {
	public UpdateObjectResult updateObject(String bucketName, String key, File file, long startOffset) throws AmazonClientException;
	public UpdateObjectResult updateObject(String bucketName, String key, InputStream input, ObjectMetadata metadata, long startOffset) throws AmazonClientException;
	public UpdateObjectResult updateObject(UpdateObjectRequest request) throws AmazonClientException;

	public AppendObjectResult appendObject(String bucketName, String key, File file) throws AmazonClientException;
	public AppendObjectResult appendObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws AmazonClientException;
	public AppendObjectResult appendObject(AppendObjectRequest request) throws AmazonClientException;

	public SetBucketFileAccessModeResult setBucketFileAccessMode(SetBucketFileAccessModeRequest request) throws AmazonClientException;

	public GetBucketFileAccessModeResult getBucketFileAccessMode(String bucketName) throws AmazonClientException;

	public GetFileAccessResult getFileAccess(GetFileAccessRequest request) throws AmazonClientException;
}
