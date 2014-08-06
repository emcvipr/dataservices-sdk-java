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
import com.amazonaws.event.ProgressListenerCallbackExecutor;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
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
import com.netflix.loadbalancer.LoadBalancerStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ViPRS3Client extends AmazonS3Client implements ViPRS3, AmazonS3 {
    private static Log log = LogFactory.getLog(ViPRS3Client.class);

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
        this(endpoint, new StaticCredentialsProvider(awsCredentials), new ClientConfiguration());
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
        this(endpoint, new StaticCredentialsProvider(awsCredentials), clientConfiguration);
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
        this(endpoint, credentialsProvider, new ClientConfiguration());
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
		super(credentialsProvider, clientConfiguration == null ? new ClientConfiguration() : clientConfiguration);
        setEndpoint(endpoint);
	}

    /**
     * Constructs a client with all options specified in a ViPRS3Config instance.  Use this constructor to enable the
     * smart client.  Older constructors cannot differentiate between load balancing and virtual hosting (which is not
     * supported by the smart client), so this constructor is required for the smart client.
     *
     * Note that when the smart client is enabled, you *cannot* use DNS (virtual host) style buckets or namespaces.
     * Because of the nature of client-side load balancing, you must use path/header style requests.
     */
    public ViPRS3Client(ViPRS3Config viprConfig) {
        super(viprConfig.getCredentialsProvider(), viprConfig.getClientConfiguration());
        this.client = new ViPRS3HttpClient(viprConfig);
        setEndpoint(viprConfig.getProtocol() + "://" + viprConfig.getVipHost());

        // enable path-style requests (cannot use DNS with client-side load balancing)
        S3ClientOptions options = new S3ClientOptions();
        options.setPathStyleAccess(true);
        setS3ClientOptions(options);
    }

    public LoadBalancerStats getLoadBalancerStats() {
        if (client instanceof ViPRS3HttpClient)
            return ((ViPRS3HttpClient) client).getLoadBalancerStats();
        throw new UnsupportedOperationException("this is not a load-balanced client (try constructing it with ViPRS3Config)");
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
        if (putAccessModeRequest.getDuration() != 0) {
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
        if (putAccessModeRequest.isPreserveIngestPaths()) {
            request.addHeader(ViPRConstants.FILE_ACCESS_PRESERVE_INGEST_PATHS, "true");
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
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_PRESERVE_INGEST_PATHS))
                    result.setPreserveIngestPaths(Boolean.parseBoolean(headers.get(ViPRConstants.FILE_ACCESS_PRESERVE_INGEST_PATHS)));

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
                    result.setEndToken(headers.get(ViPRConstants.FILE_ACCESS_END_TOKEN_HEADER));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_PRESERVE_INGEST_PATHS))
                    result.setPreserveIngestPaths(Boolean.parseBoolean(headers.get(ViPRConstants.FILE_ACCESS_PRESERVE_INGEST_PATHS)));

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

    public ListDataNodesResult listDataNodes(ListDataNodesRequest listDataNodesRequest)
            throws AmazonClientException {
        Request<ListDataNodesRequest> request = createRequest(null, null, listDataNodesRequest, HttpMethodName.GET);
        request.addParameter(ViPRConstants.ENDPOINT_PARAMETER, null);

        return invoke(request, new AbstractS3ResponseHandler<ListDataNodesResult>() {
            public AmazonWebServiceResponse<ListDataNodesResult> handle(HttpResponse response) throws Exception {
                log.trace("Beginning to parse endpoint response XML");
                ListDataNodesResult result = new ListDataNodesResultUnmarshaller().unmarshall(response.getContent());
                log.trace("Done parsing endpoint response XML");

                AmazonWebServiceResponse<ListDataNodesResult> awsResponse = parseResponseMetadata(response);
                awsResponse.setResult(result);
                return awsResponse;
            }
        }, null, null);
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
                + ((key != null) ? key : "");

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
        if (metadata == null) metadata = new ObjectMetadata();

        assertParameterNotNull(bucketName, "The bucket name parameter must be specified when uploading an object");
        assertParameterNotNull(key, "The key parameter must be specified when uploading an object");

        /*
         * This is compatible with progress listener set by either the legacy
         * method GetObjectRequest#setProgressListener or the new method
         * GetObjectRequest#setGeneralProgressListener.
         */
        com.amazonaws.event.ProgressListener progressListener = putObjectRequest.getGeneralProgressListener();
        ProgressListenerCallbackExecutor progressListenerCallbackExecutor = ProgressListenerCallbackExecutor
                .wrapListener(progressListener);

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

        if (progressListenerCallbackExecutor != null) {
            com.amazonaws.event.ProgressReportingInputStream progressReportingInputStream = new com.amazonaws.event.ProgressReportingInputStream(input, progressListenerCallbackExecutor);
            fireProgressEvent(progressListenerCallbackExecutor, com.amazonaws.event.ProgressEvent.STARTED_EVENT_CODE);
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
            fireProgressEvent(progressListenerCallbackExecutor, com.amazonaws.event.ProgressEvent.FAILED_EVENT_CODE);
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
                    fireProgressEvent(progressListenerCallbackExecutor, com.amazonaws.event.ProgressEvent.FAILED_EVENT_CODE);
                    throw new AmazonClientException("Unable to verify integrity of data upload.  " +
                            "Client calculated content hash didn't match hash calculated by Amazon S3.  " +
                            "You may need to delete the data stored in Amazon S3.");
                }
            }
        }

        fireProgressEvent(progressListenerCallbackExecutor, com.amazonaws.event.ProgressEvent.COMPLETED_EVENT_CODE);

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
        BucketNameUtils.validateBucketName(bucketName);

        Request<ViPRCreateBucketRequest> request = createRequest(bucketName, null, createBucketRequest, HttpMethodName.PUT);

        if ( createBucketRequest.getAccessControlList() != null ) {
            addAclHeaders(request, createBucketRequest.getAccessControlList());
        } else if ( createBucketRequest.getCannedAcl() != null ) {
            request.addHeader(Headers.S3_CANNED_ACL, createBucketRequest.getCannedAcl().toString());
        }

        // ViPR specific: projectId,  vpoolId and fsAccessEnabled.
        if(createBucketRequest.getProjectId() != null) {
            request.addHeader(ViPRConstants.PROJECT_HEADER, createBucketRequest.getProjectId());
        }
        if(createBucketRequest.getVpoolId() != null) {
            request.addHeader(ViPRConstants.VPOOL_HEADER, createBucketRequest.getVpoolId());
        }
        if(createBucketRequest.isFsAccessEnabled()) {
            request.addHeader(ViPRConstants.FS_ACCESS_ENABLED, "true");
        }

        /*
         * If we're talking to a region-specific endpoint other than the US, we
         * *must* specify a location constraint. Try to derive the region from
         * the endpoint.
         */
        if (!(this.endpoint.getHost().equals(Constants.S3_HOSTNAME))
                && (region == null || region.isEmpty())) {

            try {
                region = RegionUtils
                        .getRegionByEndpoint(this.endpoint.getHost())
                        .getName();
            } catch (IllegalArgumentException exception) {
                // Endpoint does not correspond to a known region; send the
                // request with no location constraint and hope for the best.
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

    public static final class ListDataNodesResultUnmarshaller implements
            Unmarshaller<ListDataNodesResult, InputStream> {

        @Override
        public ListDataNodesResult unmarshall(InputStream in) throws Exception {
            return new ViPRResponsesSaxParser().parseListDataNodeResult(in).getResult();
        }
    }
}
