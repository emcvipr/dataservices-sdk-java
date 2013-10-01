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

/**
 *
 */
package com.emc.vipr.services.s3;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.HttpResponseHandler;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.transform.Unmarshaller;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.Md5Utils;
import com.emc.vipr.services.s3.model.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.lang.Object;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author cwikj
 *
 */
public class ViPRS3Client extends AmazonS3Client implements ViPRS3, AmazonS3 {
    private static Log log = LogFactory.getLog(ViPRS3Client.class);

    /** Responsible for handling error responses from all S3 service calls. */
    protected S3ErrorResponseHandler errorResponseHandler = new S3ErrorResponseHandler();
    
    /** Shared response handler for operations with no response.  */
    private S3XmlResponseHandler<Void> voidResponseHandler = new S3XmlResponseHandler<Void>(null);
    
    /** Utilities for validating bucket names */
    private final BucketNameUtils bucketNameUtils = new BucketNameUtils();

    protected AWSCredentialsProvider awsCredentialsProvider;

    protected S3ClientOptions clientOptions = new S3ClientOptions();

	protected String namespace;

    /**
     * Constructs a new ViPR S3 client using the specified endpoint, AWS credentials to
     * access the EMC ViPR S3 protocol.
     *
     * @param endpoint
     *            The ViPR S3 endpoint (i.e. "https://vipr-data.emc.com:9021")
     * @param awsCredentials
     *            The AWS credentials to use when making requests
     *            with this client.
     *
     * @see AmazonS3Client#AmazonS3Client()
     * @see AmazonS3Client#AmazonS3Client(AWSCredentials)
     */
	public ViPRS3Client(String endpoint, AWSCredentials awsCredentials) {
		super(awsCredentials);
        setEndpoint(endpoint);

		this.awsCredentialsProvider = new StaticCredentialsProvider(awsCredentials);
	}

    /**
     * Constructs a new ViPR S3 client using the specified endpoint, AWS credentials and
     * client configuration to access the EMC ViPR S3 protocol.
     *
     * @param endpoint
     *            The ViPR S3 endpoint (i.e. "https://vipr-data.emc.com:9021")
     * @param awsCredentials
     *            The AWS credentials to use when making requests
     *            with this client.
     * @param clientConfiguration
     *            The client configuration options controlling how this client
     *            connects (e.g. proxy settings, retry counts, etc).
     *
     * @see AmazonS3Client#AmazonS3Client()
     * @see AmazonS3Client#AmazonS3Client(AWSCredentials, ClientConfiguration)
     */
	public ViPRS3Client(String endpoint, AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
		super(awsCredentials, clientConfiguration);
        setEndpoint(endpoint);

		this.awsCredentialsProvider = new StaticCredentialsProvider(awsCredentials);
	}

    /**
     * Constructs a new ViPR S3 client using the specified endpoint, AWS credentials
     * provider to access the EMC ViPR S3 protocol.
     *
     * @param endpoint
     *            The ViPR S3 endpoint (i.e. "https://vipr-data.emc.com:9021")
     * @param credentialsProvider
     *            The AWS credentials provider which will provide credentials
     *            to authenticate requests.
     * @see AmazonS3Client#AmazonS3Client(AWSCredentialsProvider)
     */
	public ViPRS3Client(String endpoint, AWSCredentialsProvider credentialsProvider) {
		super(credentialsProvider);
        setEndpoint(endpoint);

		this.awsCredentialsProvider = credentialsProvider;
	}

    /**
     * Constructs a new ViPR S3 client using the specified endpoint, AWS credentials and
     * client configuration to access the EMC ViPR S3 protocol.
     *
     * @param endpoint
     *            The ViPR S3 endpoint (i.e. "https://vipr-data.emc.com:9021")
     * @param credentialsProvider
     *            The AWS credentials provider which will provide credentials
     *            to authenticate requests.
     * @param clientConfiguration
     *            The client configuration options controlling how this client
     *            connects (e.g. proxy settings, retry counts, etc).
     */
	public ViPRS3Client(String endpoint, AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration) {
		super(credentialsProvider, clientConfiguration);
        setEndpoint(endpoint);

		this.awsCredentialsProvider = credentialsProvider;
	}

    /**
     * This allows us to extend S3ClientOptions in the future. For now, setPathStyleAccess(true) if you do not want
     * to use virtual-host-style namespaces/buckets (the default).
     */
    @Override
    public void setS3ClientOptions(S3ClientOptions clientOptions) {
        super.setS3ClientOptions(clientOptions);
        this.clientOptions = new S3ClientOptions(clientOptions);
    }

    /**
     * Copied from AmazonS3Client to override the S3ObjectResponseHandler for v1. ViPR should return a hyphen in the
     * ETag when reading multipart uploads, but in v1 it does not. This hyphen signals that the object is a multipart
     * upload and the ETag should be ignored. Here we use our own response handler to detect multipart uploads from
     * ViPR and insert a hyphen in the ETag.
     * TODO: remove post v1 when ViPR will return a hyphen in the ETag for multipart uploads
     */
    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest)
            throws AmazonClientException, AmazonServiceException {
        assertParameterNotNull(getObjectRequest,
                "The GetObjectRequest parameter must be specified when requesting an object");
        assertParameterNotNull(getObjectRequest.getBucketName(),
                "The bucket name parameter must be specified when requesting an object");
        assertParameterNotNull(getObjectRequest.getKey(),
                "The key parameter must be specified when requesting an object");

        Request<GetObjectRequest> request = createRequest(getObjectRequest.getBucketName(), getObjectRequest.getKey(), getObjectRequest, HttpMethodName.GET);

        if (getObjectRequest.getVersionId() != null) {
            request.addParameter("versionId", getObjectRequest.getVersionId());
        }

        // Range
        if (getObjectRequest.getRange() != null) {
            long[] range = getObjectRequest.getRange();
            request.addHeader(Headers.RANGE, "bytes=" + Long.toString(range[0]) + "-" + Long.toString(range[1]));
        }

        addResponseHeaderParameters(request, getObjectRequest.getResponseHeaders());

        addDateHeader(request, Headers.GET_OBJECT_IF_MODIFIED_SINCE,
                getObjectRequest.getModifiedSinceConstraint());
        addDateHeader(request, Headers.GET_OBJECT_IF_UNMODIFIED_SINCE,
                getObjectRequest.getUnmodifiedSinceConstraint());
        addStringListHeader(request, Headers.GET_OBJECT_IF_MATCH,
                getObjectRequest.getMatchingETagConstraints());
        addStringListHeader(request, Headers.GET_OBJECT_IF_NONE_MATCH,
                getObjectRequest.getNonmatchingETagConstraints());

        ProgressListener progressListener = getObjectRequest.getProgressListener();
        try {
            S3Object s3Object = invoke(request, new ViPRS3ObjectResponseHandler(), getObjectRequest.getBucketName(), getObjectRequest.getKey());

            /*
             * TODO: For now, it's easiest to set there here in the client, but
             *       we could push this back into the response handler with a
             *       little more work.
             */
            s3Object.setBucketName(getObjectRequest.getBucketName());
            s3Object.setKey(getObjectRequest.getKey());

            S3ObjectInputStream input = s3Object.getObjectContent();
            if (progressListener != null) {
                ProgressReportingInputStream progressReportingInputStream = new ProgressReportingInputStream(input, progressListener);
                progressReportingInputStream.setFireCompletedEvent(true);
                input = new S3ObjectInputStream(progressReportingInputStream, input.getHttpRequest());
                fireProgressEvent(progressListener, ProgressEvent.STARTED_EVENT_CODE);
            }

            if (getObjectRequest.getRange() == null && System.getProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation") == null) {
                byte[] serverSideHash = null;
                String etag = s3Object.getObjectMetadata().getETag();
                if (etag != null && ServiceUtils.isMultipartUploadETag(etag) == false) {
                    serverSideHash = BinaryUtils.fromHex(s3Object.getObjectMetadata().getETag());
                    DigestValidationInputStream inputStreamWithMD5DigestValidation;
                    try {
                        MessageDigest digest = MessageDigest.getInstance("MD5");
                        inputStreamWithMD5DigestValidation = new DigestValidationInputStream(input, digest, serverSideHash);
                        input = new S3ObjectInputStream(inputStreamWithMD5DigestValidation, input.getHttpRequest());
                    } catch (NoSuchAlgorithmException e) {
                        log.warn("No MD5 digest algorithm available.  Unable to calculate "
                                + "checksum and verify data integrity.", e);
                    }
                }
            }

            s3Object.setObjectContent(input);

            return s3Object;
        } catch (AmazonS3Exception ase) {
            /*
             * If the request failed because one of the specified constraints
             * was not met (ex: matching ETag, modified since date, etc.), then
             * return null, so that users don't have to wrap their code in
             * try/catch blocks and check for this status code if they want to
             * use constraints.
             */
            if (ase.getStatusCode() == 412 || ase.getStatusCode() == 304) {
                fireProgressEvent(progressListener, ProgressEvent.CANCELED_EVENT_CODE);
                return null;
            }

            fireProgressEvent(progressListener, ProgressEvent.FAILED_EVENT_CODE);
            throw ase;
        }
    }

    public UpdateObjectResult updateObject(String bucketName, String key,
            File file, long startOffset) throws AmazonClientException {
        UpdateObjectRequest request = new UpdateObjectRequest(bucketName, key,
                file).withUpdateOffset(startOffset);

        return updateObject(request);
    }

    public UpdateObjectResult updateObject(String bucketName, String key,
            InputStream input, ObjectMetadata metadata, long startOffset)
            throws AmazonClientException {
        UpdateObjectRequest request = new UpdateObjectRequest(bucketName, key,
                input, metadata).withUpdateOffset(startOffset);

        return updateObject(request);
    }

    public UpdateObjectResult updateObject(UpdateObjectRequest request)
            throws AmazonClientException {
        ObjectMetadata returnedMetadata = doPut(request);
        UpdateObjectResult result = new UpdateObjectResult();
        result.setETag(returnedMetadata.getETag());
        result.setVersionId(returnedMetadata.getVersionId());
        result.setServerSideEncryption(returnedMetadata
                .getServerSideEncryption());
        result.setExpirationTime(returnedMetadata.getExpirationTime());
        result.setExpirationTimeRuleId(returnedMetadata
                .getExpirationTimeRuleId());
        return result;
    }

    public AppendObjectResult appendObject(String bucketName, String key,
            File file) throws AmazonClientException {
        AppendObjectRequest request = new AppendObjectRequest(bucketName, key,
                file);

        return appendObject(request);
    }

    public AppendObjectResult appendObject(String bucketName, String key,
            InputStream input, ObjectMetadata metadata)
            throws AmazonClientException {
        AppendObjectRequest request = new AppendObjectRequest(bucketName, key,
                input, metadata);

        return appendObject(request);
    }

    public AppendObjectResult appendObject(AppendObjectRequest request)
            throws AmazonClientException {
        ObjectMetadata returnedMetadata = doPut(request);
        AppendObjectResult result = new AppendObjectResult();
        result.setETag(returnedMetadata.getETag());
        result.setVersionId(returnedMetadata.getVersionId());
        result.setServerSideEncryption(returnedMetadata
                .getServerSideEncryption());
        result.setExpirationTime(returnedMetadata.getExpirationTime());
        result.setExpirationTimeRuleId(returnedMetadata
                .getExpirationTimeRuleId());
        result.setAppendOffset(Long.parseLong(""
                + returnedMetadata.getRawMetadata().get(
                        ViPRConstants.APPEND_OFFSET_HEADER)));
        return result;
    }

    public BucketFileAccessModeResult setBucketFileAccessMode(SetBucketFileAccessModeRequest putAccessModeRequest)
            throws AmazonClientException {
        assertParameterNotNull(putAccessModeRequest, "The SetBucketFileAccessModeRequest parameter must be specified");

        String bucketName = putAccessModeRequest.getBucketName();
        assertParameterNotNull(bucketName, "The bucket name parameter must be specified when changing access mode");

        Request<SetBucketFileAccessModeRequest> request = createRequest(bucketName, null, putAccessModeRequest, HttpMethodName.PUT);
        request.addParameter(ViPRConstants.ACCESS_MODE_PARAMETER, null);
        request.addHeader(Headers.CONTENT_TYPE, Mimetypes.MIMETYPE_XML);

        if (putAccessModeRequest.getAccessMode() != null) {
            request.addHeader(ViPRConstants.FILE_ACCESS_MODE_HEADER, putAccessModeRequest.getAccessMode().toString());
        }
        if (putAccessModeRequest.getDuration() > 0) { // TODO: is this an appropriate indicator?
            request.addHeader(ViPRConstants.FILE_ACCESS_DURATION_HEADER, Long.toString(putAccessModeRequest.getDuration()));
        }
        if (putAccessModeRequest.getHostList() != null) {
            request.addHeader(ViPRConstants.FILE_ACCESS_HOST_LIST_HEADER, join(",", putAccessModeRequest.getHostList()));
        }
        if (putAccessModeRequest.getUid() != null) {
            request.addHeader(ViPRConstants.FILE_ACCESS_UID_HEADER, putAccessModeRequest.getUid());
        }
        if (putAccessModeRequest.getToken() != null) {
            request.addHeader(ViPRConstants.FILE_ACCESS_TOKEN_HEADER, putAccessModeRequest.getToken());
        }

        return invoke(request, new AbstractS3ResponseHandler<BucketFileAccessModeResult>() {
            public AmazonWebServiceResponse<BucketFileAccessModeResult> handle(HttpResponse response) throws Exception {
                BucketFileAccessModeResult result = new BucketFileAccessModeResult();
                Map<String, String> headers = response.getHeaders();

                if (headers.containsKey(ViPRConstants.FILE_ACCESS_MODE_HEADER))
                    result.setAccessMode(ViPRConstants.FileAccessMode.valueOf(headers.get(ViPRConstants.FILE_ACCESS_MODE_HEADER)));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_DURATION_HEADER))
                    result.setDuration(Long.parseLong(headers.get(ViPRConstants.FILE_ACCESS_DURATION_HEADER)));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_HOST_LIST_HEADER))
                    result.setHostList(Arrays.asList(headers.get(ViPRConstants.FILE_ACCESS_HOST_LIST_HEADER).split(",")));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_UID_HEADER))
                    result.setUid(headers.get(ViPRConstants.FILE_ACCESS_UID_HEADER));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_START_TOKEN_HEADER))
                    result.setStartToken(headers.get(ViPRConstants.FILE_ACCESS_START_TOKEN_HEADER));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_END_TOKEN_HEADER))
                    result.setEndToken(headers.get(ViPRConstants.FILE_ACCESS_END_TOKEN_HEADER));

                AmazonWebServiceResponse<BucketFileAccessModeResult> awsResponse = parseResponseMetadata(response);
                awsResponse.setResult(result);
                return awsResponse;
            }
        }, bucketName, null);
    }

    public BucketFileAccessModeResult getBucketFileAccessMode(String bucketName)
            throws AmazonClientException {
        assertParameterNotNull(bucketName, "The bucket name parameter must be specified when querying access mode");

        Request<GenericBucketRequest> request = createRequest(bucketName, null, new GenericBucketRequest(bucketName), HttpMethodName.GET);
        request.addParameter(ViPRConstants.ACCESS_MODE_PARAMETER, null);

        return invoke(request, new AbstractS3ResponseHandler<BucketFileAccessModeResult>() {
            public AmazonWebServiceResponse<BucketFileAccessModeResult> handle(HttpResponse response) throws Exception {
                BucketFileAccessModeResult result = new BucketFileAccessModeResult();
                Map<String, String> headers = response.getHeaders();

                if (headers.containsKey(ViPRConstants.FILE_ACCESS_MODE_HEADER))
                    result.setAccessMode(ViPRConstants.FileAccessMode.valueOf(headers.get(ViPRConstants.FILE_ACCESS_MODE_HEADER)));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_DURATION_HEADER))
                    result.setDuration(Long.parseLong(headers.get(ViPRConstants.FILE_ACCESS_DURATION_HEADER)));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_HOST_LIST_HEADER))
                    result.setHostList(Arrays.asList(headers.get(ViPRConstants.FILE_ACCESS_HOST_LIST_HEADER).split(",")));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_UID_HEADER))
                    result.setUid(headers.get(ViPRConstants.FILE_ACCESS_UID_HEADER));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_START_TOKEN_HEADER))
                    result.setStartToken(headers.get(ViPRConstants.FILE_ACCESS_START_TOKEN_HEADER));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_END_TOKEN_HEADER))
                    result.setEndToken(ViPRConstants.FILE_ACCESS_END_TOKEN_HEADER);

                AmazonWebServiceResponse<BucketFileAccessModeResult> awsResponse = parseResponseMetadata(response);
                awsResponse.setResult(result);
                return awsResponse;
            }
        }, bucketName, null);
    }

    public GetFileAccessResult getFileAccess(GetFileAccessRequest getFileAccessRequest)
            throws AmazonClientException {
        assertParameterNotNull(getFileAccessRequest, "The GetFileAccessRequest parameter must be specified");

        String bucketName = getFileAccessRequest.getBucketName();
        assertParameterNotNull(bucketName, "The bucket name parameter must be specified when querying file access");

        Request<GetFileAccessRequest> request = createRequest(bucketName, null, getFileAccessRequest, HttpMethodName.GET);
        request.addParameter(ViPRConstants.FILE_ACCESS_PARAMETER, null);

        if (getFileAccessRequest.getMarker() != null) {
            request.addParameter(ViPRConstants.MARKER_PARAMETER, getFileAccessRequest.getMarker());
        }
        if (getFileAccessRequest.getMaxKeys() > 0) { // TODO: is this an appropriate indicator?
            request.addParameter(ViPRConstants.MAX_KEYS_PARAMETER, Long.toString(getFileAccessRequest.getMaxKeys()));
        }

        return invoke(request, new AbstractS3ResponseHandler<GetFileAccessResult>() {
            public AmazonWebServiceResponse<GetFileAccessResult> handle(HttpResponse response) throws Exception {
                log.trace("Beginning to parse fileaccess response XML");
                GetFileAccessResult result = new GetFileAccessResultUnmarshaller().unmarshall(response.getContent());
                log.trace("Done parsing fileaccess response XML");

                AmazonWebServiceResponse<GetFileAccessResult> awsResponse = parseResponseMetadata(response);
                awsResponse.setResult(result);
                return awsResponse;
            }
        }, bucketName, null);
    }

    /**
     * Overridden to specify the namespace via vHost or header. This choice is consistent with the
     * bucket convention used in the standard AWS client.  vHost buckets implies vHost namespace, otherwise the
     * namespace is specified in a header.
     */
    @Override
    protected <X extends AmazonWebServiceRequest> Request<X> createRequest(String bucketName, String key, X originalRequest, HttpMethodName httpMethod) {
        Request<X> request = super.createRequest(bucketName, key, originalRequest, httpMethod);

        if (namespace != null) {
            // is this a vHost request?
            if (vHostRequest(request, bucketName)) {
                // then prepend the namespace and bucket into the request host
                request.setEndpoint(convertToVirtualHostEndpoint(namespace, bucketName));
            } else {
                // otherwise add a header for namespace
                if (!request.getHeaders().containsKey(ViPRConstants.NAMESPACE_HEADER))
                    request.addHeader(ViPRConstants.NAMESPACE_HEADER, namespace);
            }
        }

        return request;
    }

    /**
     * Overridden to provide our own signer, which will include x-emc headers and namespace in the signature.
     */
    @Override
    protected Signer createSigner(Request<?> request, String bucketName, String key) {
        String resourcePath = "/" + ((bucketName != null) ? bucketName + "/" : "")
                + ((key != null) ? ServiceUtils.urlEncode(key) : "");

        // if we're using a vHost request, the namespace must be prepended to the resource path when signing
        if (namespace != null && vHostRequest(request, bucketName)) {
            resourcePath = "/" + namespace + resourcePath;
        }

        return new ViPRS3Signer(request.getHttpMethod().toString(), resourcePath);
    }

    protected boolean vHostRequest(Request<?> request, String bucketName) {
        return request.getResourcePath() == null || !request.getResourcePath().startsWith(bucketName + "/");
    }

    /**
     * Converts the current endpoint set for this client into virtual addressing
     * style, by placing the name of the specified bucket and namespace before the S3
     * endpoint.
     *
     * Convention is bucket.namespace.root-host (i.e. my-bucket.my-namespace.vipr-s3.my-company.com)
     *
     * @param namespace
     *            The namespace to use in the virtual addressing style
     *            of the returned URI.
     * @param bucketName
     *            The name of the bucket to use in the virtual addressing style
     *            of the returned URI.
     *
     * @return A new URI, creating from the current service endpoint URI and the
     *         specified namespace and bucket.
     */
    protected URI convertToVirtualHostEndpoint(String namespace, String bucketName) {
        try {
            return new URI(endpoint.getScheme() + "://" + bucketName + "." + namespace + "." + endpoint.getAuthority());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid namespace or bucket name: " + bucketName + "." + namespace, e);
        }
    }

    /**
     * Executes a (Subclass of) PutObjectRequest.  In particular, we check for subclasses
     * of the UpdateObjectRequest and inject the value of the Range header.  This version
     * also returns the raw ObjectMetadata for the response so callers can construct
     * their own result objects.
     * @param putObjectRequest the request to execute
     * @return an ObjectMetadata containing the response headers.
     */
    protected ObjectMetadata doPut(PutObjectRequest putObjectRequest) {
        assertParameterNotNull(putObjectRequest, "The PutObjectRequest parameter must be specified when uploading an object");

        String bucketName = putObjectRequest.getBucketName();
        String key = putObjectRequest.getKey();
        ObjectMetadata metadata = putObjectRequest.getMetadata();
        InputStream input = putObjectRequest.getInputStream();
        ProgressListener progressListener = putObjectRequest.getProgressListener();
        if (metadata == null) metadata = new ObjectMetadata();

        assertParameterNotNull(bucketName, "The bucket name parameter must be specified when uploading an object");
        assertParameterNotNull(key, "The key parameter must be specified when uploading an object");

        // If a file is specified for upload, we need to pull some additional
        // information from it to auto-configure a few options
        if (putObjectRequest.getFile() != null) {
            File file = putObjectRequest.getFile();

            // Always set the content length, even if it's already set
            metadata.setContentLength(file.length());

            // Only set the content type if it hasn't already been set
            if (metadata.getContentType() == null) {
                metadata.setContentType(Mimetypes.getInstance().getMimetype(file));
            }

            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(file);
                byte[] md5Hash = Md5Utils.computeMD5Hash(fileInputStream);
                metadata.setContentMD5(BinaryUtils.toBase64(md5Hash));
            } catch (Exception e) {
                throw new AmazonClientException(
                        "Unable to calculate MD5 hash: " + e.getMessage(), e);
            } finally {
                try {fileInputStream.close();} catch (Exception e) {}
            }

            try {
                input = new RepeatableFileInputStream(file);
            } catch (FileNotFoundException fnfe) {
                throw new AmazonClientException("Unable to find file to upload", fnfe);
            }
        }

        Request<PutObjectRequest> request = createRequest( bucketName, key, putObjectRequest, HttpMethodName.PUT );

        if ( putObjectRequest.getAccessControlList() != null) {
            addAclHeaders(request, putObjectRequest.getAccessControlList());
        } else if ( putObjectRequest.getCannedAcl() != null ) {
            request.addHeader(Headers.S3_CANNED_ACL, putObjectRequest.getCannedAcl().toString());
        }

        if (putObjectRequest.getStorageClass() != null) {
            request.addHeader(Headers.STORAGE_CLASS, putObjectRequest.getStorageClass());
        }

        if (putObjectRequest.getRedirectLocation() != null) {
            request.addHeader(Headers.REDIRECT_LOCATION, putObjectRequest.getRedirectLocation());
            if (input == null) {
                input = new ByteArrayInputStream(new byte[0]);
            }
        }

        // Use internal interface to differentiate 0 from unset.
        if (metadata.getRawMetadata().get(Headers.CONTENT_LENGTH) == null) {
            /*
             * There's nothing we can do except for let the HTTP client buffer
             * the input stream contents if the caller doesn't tell us how much
             * data to expect in a stream since we have to explicitly tell
             * Amazon S3 how much we're sending before we start sending any of
             * it.
             */
            log.warn("No content length specified for stream data.  " +
                     "Stream contents will be buffered in memory and could result in " +
                     "out of memory errors.");
        }

        if (progressListener != null) {
            input = new ProgressReportingInputStream(input, progressListener);
            fireProgressEvent(progressListener, ProgressEvent.STARTED_EVENT_CODE);
        }

        if (!input.markSupported()) {
            int streamBufferSize = Constants.DEFAULT_STREAM_BUFFER_SIZE;
            String bufferSizeOverride = System.getProperty("com.amazonaws.sdk.s3.defaultStreamBufferSize");
            if (bufferSizeOverride != null) {
                try {
                    streamBufferSize = Integer.parseInt(bufferSizeOverride);
                } catch (Exception e) {
                    log.warn("Unable to parse buffer size override from value: " + bufferSizeOverride);
                }
            }

            input = new RepeatableInputStream(input, streamBufferSize);
        }

        MD5DigestCalculatingInputStream md5DigestStream = null;
        if (metadata.getContentMD5() == null) {
            /*
             * If the user hasn't set the content MD5, then we don't want to
             * buffer the whole stream in memory just to calculate it. Instead,
             * we can calculate it on the fly and validate it with the returned
             * ETag from the object upload.
             */
            try {
                md5DigestStream = new MD5DigestCalculatingInputStream(input);
                input = md5DigestStream;
            } catch (NoSuchAlgorithmException e) {
                log.warn("No MD5 digest algorithm available.  Unable to calculate " +
                         "checksum and verify data integrity.", e);
            }
        }

        if (metadata.getContentType() == null) {
            /*
             * Default to the "application/octet-stream" if the user hasn't
             * specified a content type.
             */
            metadata.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);
        }

        populateRequestMetadata(request, metadata);
        request.setContent(input);

        if(putObjectRequest instanceof UpdateObjectRequest) {
            request.addHeader(Headers.RANGE,
                    "bytes=" + ((UpdateObjectRequest)putObjectRequest).getUpdateRange());
        }

        ObjectMetadata returnedMetadata = null;
        try {
            returnedMetadata = invoke(request, new S3MetadataResponseHandler(), bucketName, key);
        } catch (AmazonClientException ace) {
            fireProgressEvent(progressListener, ProgressEvent.FAILED_EVENT_CODE);
            throw ace;
        } finally {
            try {input.close();} catch (Exception e) {
                log.warn("Unable to cleanly close input stream: " + e.getMessage(), e);
            }
        }

        String contentMd5 = metadata.getContentMD5();
        if (md5DigestStream != null) {
            contentMd5 = BinaryUtils.toBase64(md5DigestStream.getMd5Digest());
        }

        // Can't verify MD5 on appends/update (yet).
        if(!(putObjectRequest instanceof UpdateObjectRequest)) {
            if (returnedMetadata != null && contentMd5 != null) {
                byte[] clientSideHash = BinaryUtils.fromBase64(contentMd5);
                byte[] serverSideHash = BinaryUtils.fromHex(returnedMetadata.getETag());

                if (!Arrays.equals(clientSideHash, serverSideHash)) {
                    fireProgressEvent(progressListener, ProgressEvent.FAILED_EVENT_CODE);
                    throw new AmazonClientException("Unable to verify integrity of data upload.  " +
                            "Client calculated content hash didn't match hash calculated by Amazon S3.  " +
                            "You may need to delete the data stored in Amazon S3.");
                }
            }
        }

        fireProgressEvent(progressListener, ProgressEvent.COMPLETED_EVENT_CODE);

        return returnedMetadata;
	}


	/**
	 * Gets the current ViPR namespace for this client.
	 * @return the ViPR namespace associated with this client
	 */
	public String getNamespace() {
		return namespace;
	}

    /**
     * Sets the ViPR namespace to use for this client.
     *
     * @param namespace the namespace to set
     */
    public synchronized void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Copied verbatim from AmazonS3Client only because it is private and we call it from our extension methods for consistency.
     */
    protected void assertParameterNotNull(Object parameterValue, String errorMessage) {
        if (parameterValue == null) throw new IllegalArgumentException(errorMessage);
    }

    /**
     * Copied verbatim from AmazonS3Client only because it is private and we call it from our extension methods for consistency.
     */
    protected static void addAclHeaders(Request<? extends AmazonWebServiceRequest> request, AccessControlList acl) {
        Set<Grant> grants = acl.getGrants();
        Map<Permission, Collection<Grantee>> grantsByPermission = new HashMap<Permission, Collection<Grantee>>();
        for ( Grant grant : grants ) {
            if ( !grantsByPermission.containsKey(grant.getPermission()) ) {
                grantsByPermission.put(grant.getPermission(), new LinkedList<Grantee>());
            }
            grantsByPermission.get(grant.getPermission()).add(grant.getGrantee());
        }
        for ( Permission permission : Permission.values() ) {
            if ( grantsByPermission.containsKey(permission) ) {
                Collection<Grantee> grantees = grantsByPermission.get(permission);
                boolean seenOne = false;
                StringBuilder granteeString = new StringBuilder();
                for ( Grantee grantee : grantees ) {
                    if ( !seenOne )
                        seenOne = true;
                    else
                        granteeString.append(", ");
                    granteeString.append(grantee.getTypeIdentifier()).append("=").append("\"")
                            .append(grantee.getIdentifier()).append("\"");
                }
                request.addHeader(permission.getHeaderName(), granteeString.toString());
            }
        }
    }

    /**
     * Copied verbatim from AmazonS3Client only because it is private and we call it from our extension methods for consistency.
     */
    protected void fireProgressEvent(ProgressListener listener, int eventType) {
        if (listener == null) return;
        ProgressEvent event = new ProgressEvent(0);
        event.setEventCode(eventType);
        listener.progressChanged(event);
    }

    /**
     * Copied verbatim from AmazonS3Client only because it is private and we call it from our extension methods for consistency.
     */
    protected <X, Y extends AmazonWebServiceRequest> X invoke(Request<Y> request, HttpResponseHandler<AmazonWebServiceResponse<X>> responseHandler, String bucket, String key) {
        for (Entry<String, String> entry : request.getOriginalRequest().copyPrivateRequestParameters().entrySet()) {
            request.addParameter(entry.getKey(), entry.getValue());
        }
        request.setTimeOffset(timeOffset);

        /*
         * The string we sign needs to include the exact headers that we
         * send with the request, but the client runtime layer adds the
         * Content-Type header before the request is sent if one isn't set, so
         * we have to set something here otherwise the request will fail.
         */
        if (request.getHeaders().get("Content-Type") == null) {
            request.addHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        }

        AWSCredentials credentials = awsCredentialsProvider.getCredentials();
        AmazonWebServiceRequest originalRequest = request.getOriginalRequest();
        if (originalRequest != null && originalRequest.getRequestCredentials() != null) {
            credentials = originalRequest.getRequestCredentials();
        }

        ExecutionContext executionContext = createExecutionContext();
        executionContext.setSigner(createSigner(request, bucket, key));
        executionContext.setCredentials(credentials);

        return client.execute(request, responseHandler, errorResponseHandler, executionContext);
    }
    
    /**
     * ViPR-specific create bucket command.  This version of the command adds some
     * options specific to EMC ViPR, specifically the ability to set the ViPR project ID
     * and Object Virtual Pool ID on the new bucket.
     * @param createBucketRequest the configuration parameters for the new bucket. 
     */
    public Bucket createBucket(ViPRCreateBucketRequest createBucketRequest)
            throws AmazonClientException, AmazonServiceException {
        assertParameterNotNull(createBucketRequest,
                "The CreateBucketRequest parameter must be specified when creating a bucket");

        String bucketName = createBucketRequest.getBucketName();
        String region = createBucketRequest.getRegion();
        assertParameterNotNull(bucketName,
                "The bucket name parameter must be specified when creating a bucket");

        if (bucketName != null) bucketName = bucketName.trim();
        bucketNameUtils.validateBucketName(bucketName);

        Request<ViPRCreateBucketRequest> request = createRequest(bucketName, null, createBucketRequest, HttpMethodName.PUT);

        if ( createBucketRequest.getAccessControlList() != null ) {
            addAclHeaders(request, createBucketRequest.getAccessControlList());
        } else if ( createBucketRequest.getCannedAcl() != null ) {
            request.addHeader(Headers.S3_CANNED_ACL, createBucketRequest.getCannedAcl().toString());
        }
        
        // ViPR specific: projectId and vpoolId.
        if(createBucketRequest.getProjectId() != null) {
            request.addHeader(ViPRConstants.PROJECT_HEADER, createBucketRequest.getProjectId());
        }
        if(createBucketRequest.getVpoolId() != null) {
            request.addHeader(ViPRConstants.VPOOL_HEADER, createBucketRequest.getVpoolId());
        }

        /*
         * If we're talking to a region-specific endpoint other than the US, we
         * *must* specify a location constraint. Try to derive the region from
         * the endpoint.
         */
        if ( region == null ) {
            String endpoint = this.endpoint.getHost();
            if ( endpoint.contains("us-west-1") ) {
                region = Region.US_West.toString();
            } else if ( endpoint.contains("us-west-2") ) {
                region = Region.US_West_2.toString();
            } else if ( endpoint.contains("eu-west-1") ) {
                region = Region.EU_Ireland.toString();
            } else if ( endpoint.contains("ap-southeast-1") ) {
                region = Region.AP_Singapore.toString();
            } else if ( endpoint.contains("ap-northeast-1") ) {
                region = Region.AP_Tokyo.toString();
            } else if ( endpoint.contains("sa-east-1") ) {
                region = Region.SA_SaoPaulo.toString();
            }
        }

        /*
         * We can only send the CreateBucketConfiguration if we're *not*
         * creating a bucket in the US region.
         */
        if (region != null && !region.toUpperCase().equals(Region.US_Standard.toString())) {
            XmlWriter xml = new XmlWriter();
            xml.start("CreateBucketConfiguration", "xmlns", Constants.XML_NAMESPACE);
            xml.start("LocationConstraint").value(region).end();
            xml.end();

            request.setContent(new ByteArrayInputStream(xml.getBytes()));
        }

        invoke(request, voidResponseHandler, bucketName, null);

        return new Bucket(bucketName);
    }


    /**
     * Copied verbatim from AmazonS3Client only because it is private and we call it from our overridden getObject().
     * TODO: remove post v1 when ViPR will return a hyphen in the ETag for multipart uploads
     */
    protected static void addResponseHeaderParameters(Request<?> request, ResponseHeaderOverrides responseHeaders) {
        if ( responseHeaders != null ) {
            if ( responseHeaders.getCacheControl() != null ) {
                request.addParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CACHE_CONTROL, responseHeaders.getCacheControl());
            }
            if ( responseHeaders.getContentDisposition() != null ) {
                request.addParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_DISPOSITION,
                        responseHeaders.getContentDisposition());
            }
            if ( responseHeaders.getContentEncoding() != null ) {
                request.addParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_ENCODING,
                        responseHeaders.getContentEncoding());
            }
            if ( responseHeaders.getContentLanguage() != null ) {
                request.addParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_LANGUAGE,
                        responseHeaders.getContentLanguage());
            }
            if ( responseHeaders.getContentType() != null ) {
                request.addParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_TYPE, responseHeaders.getContentType());
            }
            if ( responseHeaders.getExpires() != null ) {
                request.addParameter(ResponseHeaderOverrides.RESPONSE_HEADER_EXPIRES, responseHeaders.getExpires());
            }
        }
    }

    /**
     * Copied verbatim from AmazonS3Client only because it is private and we call it from our overridden getObject().
     * TODO: remove post v1 when ViPR will return a hyphen in the ETag for multipart uploads
     */
    protected static void addDateHeader(Request<?> request, String header, Date value) {
        if (value != null) {
            request.addHeader(header, ServiceUtils.formatRfc822Date(value));
        }
    }

    /**
     * Copied verbatim from AmazonS3Client only because it is private and we call it from our overridden getObject().
     * TODO: remove post v1 when ViPR will return a hyphen in the ETag for multipart uploads
     */
    protected static void addStringListHeader(Request<?> request, String header, List<String> values) {
        if (values != null && !values.isEmpty()) {
            request.addHeader(header, ServiceUtils.join(values));
        }
    }

    protected String join(String delimiter, List<?> values) {
        return join(delimiter, values.toArray());
    }

    protected String join(String delimiter, Object... values) {
        StringBuilder joined = new StringBuilder();
        for (Object value : values) {
            joined.append(value).append(delimiter);
        }
        return joined.substring(0, joined.length() - 1);
    }

    public static final class GetFileAccessResultUnmarshaller implements
            Unmarshaller<GetFileAccessResult, InputStream> {

        public GetFileAccessResult unmarshall(InputStream in) throws Exception {
            return new ViPRResponsesSaxParser().parseFileAccessResult(in).getResult();
        }
    }
}
