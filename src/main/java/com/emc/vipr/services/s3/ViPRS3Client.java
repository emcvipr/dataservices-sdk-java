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
    private S3ErrorResponseHandler errorResponseHandler = new S3ErrorResponseHandler();

    private AWSCredentialsProvider awsCredentialsProvider;
    private NamespaceRequestHandler nsRequestHandler;

	private String namespace;

    /**
     * Constructs a new ViPR S3 client using the specified AWS credentials to
     * access the EMC ViPR S3 protocol.
     *
     * @param awsCredentials
     *            The AWS credentials to use when making requests
     *            with this client.
     *
     * @see AmazonS3Client#AmazonS3Client()
     * @see AmazonS3Client#AmazonS3Client(AWSCredentials)
     */
	public ViPRS3Client(AWSCredentials awsCredentials) {
		super(awsCredentials);

		this.awsCredentialsProvider = new StaticCredentialsProvider(awsCredentials);
	}

    /**
     * Constructs a new ViPR S3 client using the specified AWS credentials and
     * client configuration to access the EMC ViPR S3 protocol.
     *
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
	public ViPRS3Client(AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
		super(awsCredentials, clientConfiguration);

		this.awsCredentialsProvider = new StaticCredentialsProvider(awsCredentials);
	}

    /**
     * Constructs a new ViPR S3 client using the specified AWS credentials
     * provider to access the EMC ViPR S3 protocol.
     *
     * @param credentialsProvider
     *            The AWS credentials provider which will provide credentials
     *            to authenticate requests.
     * @see AmazonS3Client#AmazonS3Client(AWSCredentialsProvider)
     */
	public ViPRS3Client(AWSCredentialsProvider credentialsProvider) {
		super(credentialsProvider);

		this.awsCredentialsProvider = credentialsProvider;
	}

    /**
     * Constructs a new ViPR S3 client using the specified AWS credentials and
     * client configuration to access the EMC ViPR S3 protocol.
     *
     * @param credentialsProvider
     *            The AWS credentials provider which will provide credentials
     *            to authenticate requests.
     * @param clientConfiguration
     *            The client configuration options controlling how this client
     *            connects (e.g. proxy settings, retry counts, etc).
     */
	public ViPRS3Client(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration) {
		super(credentialsProvider, clientConfiguration);

		this.awsCredentialsProvider = credentialsProvider;
	}



    public UpdateObjectResult updateObject(String bucketName, String key,
            File file, long startOffset) throws AmazonClientException,
            AmazonServiceException {
        UpdateObjectRequest request = new UpdateObjectRequest(bucketName, key,
                file).withUpdateOffset(startOffset);

        return updateObject(request);
    }

    public UpdateObjectResult updateObject(String bucketName, String key,
            InputStream input, ObjectMetadata metadata, long startOffset)
            throws AmazonClientException, AmazonServiceException {
        UpdateObjectRequest request = new UpdateObjectRequest(bucketName, key,
                input, metadata).withUpdateOffset(startOffset);

        return updateObject(request);
    }

    public UpdateObjectResult updateObject(UpdateObjectRequest request)
            throws AmazonClientException, AmazonServiceException {
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
            File file) throws AmazonClientException, AmazonServiceException {
        AppendObjectRequest request = new AppendObjectRequest(bucketName, key,
                file);

        return appendObject(request);
    }

    public AppendObjectResult appendObject(String bucketName, String key,
            InputStream input, ObjectMetadata metadata)
            throws AmazonClientException, AmazonServiceException {
        AppendObjectRequest request = new AppendObjectRequest(bucketName, key,
                input, metadata);

        return appendObject(request);
    }

    public AppendObjectResult appendObject(AppendObjectRequest request)
            throws AmazonClientException, AmazonServiceException {
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

    public SetBucketFileAccessModeResult setBucketFileAccessMode(SetBucketFileAccessModeRequest putAccessModeRequest)
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
        if (putAccessModeRequest.getAccessProtocol() != null) {
            request.addHeader(ViPRConstants.FILE_ACCESS_PROTOCOL_HEADER, putAccessModeRequest.getAccessProtocol().toString());
        }
        if (putAccessModeRequest.getFileAccessDuration() > 0) { // TODO: is this an appropriate indicator?
            request.addHeader(ViPRConstants.FILE_ACCESS_DURATION_HEADER, Long.toString(putAccessModeRequest.getFileAccessDuration()));
        }
        if (putAccessModeRequest.getHostList() != null) {
            request.addHeader(ViPRConstants.HOST_LIST_HEADER, join(",", putAccessModeRequest.getHostList()));
        }
        if (putAccessModeRequest.getUser() != null) {
            request.addHeader(ViPRConstants.USER_HEADER, putAccessModeRequest.getUser());
        }
        if (putAccessModeRequest.getToken() != null) {
            request.addHeader(ViPRConstants.TOKEN_HEADER, putAccessModeRequest.getToken());
        }

        return invoke(request, new AbstractS3ResponseHandler<SetBucketFileAccessModeResult>() {
            public AmazonWebServiceResponse<SetBucketFileAccessModeResult> handle(HttpResponse response) throws Exception {
                SetBucketFileAccessModeResult result = new SetBucketFileAccessModeResult();
                Map<String, String> headers = response.getHeaders();

                if (headers.containsKey(ViPRConstants.FILE_ACCESS_MODE_HEADER))
                    result.setAccessMode(ViPRConstants.FileAccessMode.valueOf(headers.get(ViPRConstants.FILE_ACCESS_MODE_HEADER)));
                if (headers.containsKey(ViPRConstants.FILE_ACCESS_DURATION_HEADER))
                    result.setFileAccessDuration(Long.parseLong(headers.get(ViPRConstants.FILE_ACCESS_DURATION_HEADER)));
                if (headers.containsKey(ViPRConstants.HOST_LIST_HEADER))
                    result.setHostList(Arrays.asList(headers.get(ViPRConstants.HOST_LIST_HEADER).split(",")));
                if (headers.containsKey(ViPRConstants.USER_HEADER))
                    result.setUser(headers.get(ViPRConstants.USER_HEADER));
                if (headers.containsKey(ViPRConstants.TOKEN_HEADER))
                    result.setToken(headers.get(ViPRConstants.TOKEN_HEADER));

                AmazonWebServiceResponse<SetBucketFileAccessModeResult> awsResponse = parseResponseMetadata(response);
                awsResponse.setResult(result);
                return awsResponse;
            }
        }, bucketName, null);
    }

    public GetBucketFileAccessModeResult getBucketFileAccessMode(String bucketName)
            throws AmazonClientException {
        assertParameterNotNull(bucketName, "The bucket name parameter must be specified when querying access mode");

        Request<GenericBucketRequest> request = createRequest(bucketName, null, new GenericBucketRequest(bucketName), HttpMethodName.GET);
        request.addParameter(ViPRConstants.ACCESS_MODE_PARAMETER, null);

        return invoke(request, new AbstractS3ResponseHandler<GetBucketFileAccessModeResult>() {
            public AmazonWebServiceResponse<GetBucketFileAccessModeResult> handle(HttpResponse response) throws Exception {
                GetBucketFileAccessModeResult result = new GetBucketFileAccessModeResult();
                Map<String, String> headers = response.getHeaders();

                if (headers.containsKey(ViPRConstants.FILE_ACCESS_MODE_HEADER))
                    result.setAccessMode(ViPRConstants.FileAccessMode.valueOf(headers.get(ViPRConstants.FILE_ACCESS_MODE_HEADER)));

                AmazonWebServiceResponse<GetBucketFileAccessModeResult> awsResponse = parseResponseMetadata(response);
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

        Request<GetFileAccessRequest> request = createRequest(bucketName, null, getFileAccessRequest, HttpMethodName.PUT);
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

                Map<String, String> headers = response.getHeaders();
                if (headers.containsKey(ViPRConstants.BUCKET_FILE_ACCESS_HEADER))
                    result.setAccessMode(ViPRConstants.FileAccessMode.valueOf(headers.get(ViPRConstants.BUCKET_FILE_ACCESS_HEADER)));
                if (headers.containsKey(ViPRConstants.BUCKET_FILE_ACCESS_PROTOCOL_HEADER))
                    result.setAccessProtocol(ViPRConstants.FileAccessProtocol.valueOf(headers.get(ViPRConstants.BUCKET_FILE_ACCESS_PROTOCOL_HEADER)));
                if (headers.containsKey(ViPRConstants.BUCKET_ACCESS_RESTRICTION_DURATION_HEADER))
                    result.setFileAccessDuration(Long.parseLong(headers.get(ViPRConstants.BUCKET_ACCESS_RESTRICTION_DURATION_HEADER)));
                if (headers.containsKey(ViPRConstants.HOST_LIST_HEADER))
                    result.setHosts(Arrays.asList(headers.get(ViPRConstants.HOST_LIST_HEADER).split(",")));
                if (headers.containsKey(ViPRConstants.USER_HEADER))
                    result.setUser(headers.get(ViPRConstants.USER_HEADER));

                AmazonWebServiceResponse<GetFileAccessResult> awsResponse = parseResponseMetadata(response);
                awsResponse.setResult(result);
                return awsResponse;
            }
        }, bucketName, null);
    }

    @Override
    protected Signer createSigner(Request<?> request, String bucketName, String key) {
        String resourcePath = "/" + ((bucketName != null) ? bucketName + "/" : "")
                + ((key != null) ? ServiceUtils.urlEncode(key) : "");

        return new ViPRS3Signer(request.getHttpMethod().toString(), resourcePath);
    }

    /**
     * Executes a (Subclass of) PutObjectRequest.  In particular, we check for subclasses
     * of the UpdateObjectRequest and inject the value of the Range header.  This version
     * also returns the raw ObjectMetadata for the response so callers can construct
     * their own result objects.
     * @param putObjectRequest the request to execute
     * @return an ObjectMetadata containing the response headers.
     */
    private ObjectMetadata doPut(PutObjectRequest putObjectRequest) {
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
	 * Gets the currently configured ViPR namespace.  If null, the
	 * namespace will be automatically selected from the endpoint
	 * hostname.
	 * @return the current ViPR namespace
	 */
	public String getNamespace() {
		return namespace;
	}

    /**
     * Sets the ViPR namespace to use. Generally, this will be automatically
     * determined by the endpoint of the underlying S3 client in the form of
     * {namespace}.company.com, but if this is not possible, it can be
     * overridden by setting this property.
     * 
     * @param namespace
     *            the namespace to set
     */
    public synchronized void setNamespace(String namespace) {
        this.namespace = namespace;
        if (nsRequestHandler != null) {
            // Remove the old handler
            removeRequestHandler(nsRequestHandler);
            nsRequestHandler = null;
        }
        if (namespace != null) {
            // Create a new handler
            nsRequestHandler = new NamespaceRequestHandler(namespace);
            addRequestHandler(nsRequestHandler);
        }
    }

    /**
     * <p>
     * Asserts that the specified parameter value is not <code>null</code> and if it is,
     * throws an <code>IllegalArgumentException</code> with the specified error message.
     * </p>
     *
     * @param parameterValue
     *            The parameter value being checked.
     * @param errorMessage
     *            The error message to include in the IllegalArgumentException
     *            if the specified parameter is null.
     */
    private void assertParameterNotNull(Object parameterValue, String errorMessage) {
        if (parameterValue == null) throw new IllegalArgumentException(errorMessage);
    }

    /**
     * Sets the acccess control headers for the request given.
     */
    private static void addAclHeaders(Request<? extends AmazonWebServiceRequest> request, AccessControlList acl) {
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
     * Fires a progress event with the specified event type to the specified
     * listener.
     *
     * @param listener
     *            The listener to receive the event.
     * @param eventType
     *            The type of event to fire.
     */
    private void fireProgressEvent(ProgressListener listener, int eventType) {
        if (listener == null) return;
        ProgressEvent event = new ProgressEvent(0);
        event.setEventCode(eventType);
        listener.progressChanged(event);
    }

    private <X, Y extends AmazonWebServiceRequest> X invoke(Request<Y> request, HttpResponseHandler<AmazonWebServiceResponse<X>> responseHandler, String bucket, String key) {
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

    private String join(String delimiter, Object... values) {
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
