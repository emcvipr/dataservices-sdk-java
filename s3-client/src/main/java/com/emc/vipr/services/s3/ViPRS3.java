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
package com.emc.vipr.services.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.emc.vipr.services.s3.model.*;

import java.io.File;
import java.io.InputStream;

/**
 * This interface contains the ViPR extensions above and beyond standard S3
 * functionality. This includes object update, object append, and file access
 * (e.g. NFS).
 */
public interface ViPRS3 {
    /**
     * Updates an existing object. The given file will be overlaid on the
     * existing object starting at startOffset.
     * 
     * @param bucketName
     *            The name of an existing bucket, to which you have
     *            {@link Permission#Write} permission.
     * @param key
     *            The key in the bucket to update.
     * @param file
     *            The file containing the data to use for the update.
     * @param startOffset
     *            The starting offset within the object to apply the update.
     * @return A {@link UpdateObjectResult} containing the data returned from
     *         ViPR.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public UpdateObjectResult updateObject(String bucketName, String key,
            File file, long startOffset) throws AmazonClientException;

    /**
     * Updates an existing object. The given stream will be overlaid on the
     * existing object starting at startOffset.
     * 
     * @param bucketName
     *            The name of an existing bucket, to which you have
     *            {@link Permission#Write} permission.
     * @param key
     *            The key in the bucket to update.
     * @param input
     *            The input stream containing the data to be uploaded.
     * @param metadata
     *            Additional metadata for the upload. Generally, the most
     *            important property here is the Content-Length of the input
     *            stream. If you do not size the input stream, the client will
     *            be forced to buffer the entire stream into memory and size it
     *            before uploading. This can lead to {@link OutOfMemoryError}s.
     * @param startOffset
     *            The starting offset within the object to apply the update.
     * @return A {@link UpdateObjectResult} containing the data returned from
     *         ViPR.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public UpdateObjectResult updateObject(String bucketName, String key,
            InputStream input, ObjectMetadata metadata, long startOffset)
            throws AmazonClientException;

    /**
     * Updates an existing object. The given data will be overlaid on the
     * existing object starting at startOffset.
     * 
     * @param request
     *            The configured {@link UpdateObjectRequest} to execute.
     * @return A {@link UpdateObjectResult} containing the data returned from
     *         ViPR.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public UpdateObjectResult updateObject(UpdateObjectRequest request)
            throws AmazonClientException;

    /**
     * Appends to an existing object. The given file will be appended to the end
     * of the object. Note that multiple append requests may be executed
     * concurrently and will be executed serially. Therefore, you must check the
     * actual append offset in the response, see
     * {@link AppendObjectResult#getAppendOffset()}. If you must append at an
     * exact location, use updateObject but note in that case with concurrent
     * writes, the last writer will "win".
     * 
     * @param bucketName
     *            The name of an existing bucket, to which you have
     *            {@link Permission#Write} permission.
     * @param key
     *            The key in the bucket to append to.
     * @param file
     *            The file containing the data to use for the append.
     * @return A {@link AppendObjectResult} containing the data returned from
     *         ViPR including the actual append offset.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public AppendObjectResult appendObject(String bucketName, String key,
            File file) throws AmazonClientException;

    /**
     * Appends to an existing object. The given stream will be appended to the
     * end of the object. Note that multiple append requests may be executed
     * concurrently and will be executed serially. Therefore, you must check the
     * actual append offset in the response, see
     * {@link AppendObjectResult#getAppendOffset()}. If you must append at an
     * exact location, use updateObject but note in that case with concurrent
     * writes, the last writer will "win".
     * 
     * @param bucketName
     *            The name of an existing bucket, to which you have
     *            {@link Permission#Write} permission.
     * @param key
     *            The key in the bucket to append to.
     * @param input
     *            The input stream containing the data to be appended.
     * @param metadata
     *            Additional metadata for the upload. Generally, the most
     *            important property here is the Content-Length of the input
     *            stream. If you do not size the input stream, the client will
     *            be forced to buffer the entire stream into memory and size it
     *            before sending. This can lead to {@link OutOfMemoryError}s.
     * @return A {@link AppendObjectResult} containing the data returned from
     *         ViPR including the actual append offset.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public AppendObjectResult appendObject(String bucketName, String key,
            InputStream input, ObjectMetadata metadata)
            throws AmazonClientException;

    /**
     * Appends to an existing object. The given data will be appended to the end
     * of the object. Note that multiple append requests may be executed
     * concurrently and will be executed serially. Therefore, you must check the
     * actual append offset in the response, see
     * {@link AppendObjectResult#getAppendOffset()}. If you must append at an
     * exact location, use updateObject but note in that case with concurrent
     * writes, the last writer will "win".
     * 
     * @param request
     *            the configured {@link AppendObjectRequest} to execute.
     * @return A {@link AppendObjectResult} containing the data returned from
     *         ViPR including the actual append offset.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public AppendObjectResult appendObject(AppendObjectRequest request)
            throws AmazonClientException;

    /**
     * Initiates a set file access mode request on a bucket.
     * 
     * @param request
     *            The configured {@link SetBucketFileAccessModeRequest} request
     *            to execute.
     * @return A {@link com.emc.vipr.services.s3.model.BucketFileAccessModeResult}
     *         containing the results of the access mode change.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request. Generally, this will happen if the given transition
     *             is not valid.
     */
    public BucketFileAccessModeResult setBucketFileAccessMode(
            SetBucketFileAccessModeRequest request)
            throws AmazonClientException;

    /**
     * Checks the current file access mode on a bucket.
     * 
     * @param bucketName
     *            the name of the bucket to check.
     * @return a {@link BucketFileAccessModeResult} object containing the
     *         current file access mode for the bucket.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public BucketFileAccessModeResult getBucketFileAccessMode(
            String bucketName) throws AmazonClientException;

    /**
     * Gets file access information for the keys in a bucket. This includes the
     * current state of the key and the location on the file server to access
     * the object. Note that this request is paginated so be sure to check the
     * results to see if there are more results with
     * {@link GetFileAccessResult#isTruncated()}.
     * 
     * @param request
     *            the configured {@link GetFileAccessRequest} request to
     *            execute.
     * @return a {@link GetFileAccessResult} object containing the object file
     *         access information.
     * @throws AmazonClientException
     *             If any errors are encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in on the server while processing the
     *             request.
     */
    public GetFileAccessResult getFileAccess(GetFileAccessRequest request)
            throws AmazonClientException;
}
