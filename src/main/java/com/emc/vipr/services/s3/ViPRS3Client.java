/**
 * 
 */
package com.emc.vipr.services.s3;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.Request;
import com.amazonaws.handlers.RequestHandler;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.util.TimingInfo;
import com.emc.vipr.services.s3.model.AppendObjectRequest;
import com.emc.vipr.services.s3.model.AppendObjectResult;
import com.emc.vipr.services.s3.model.GetAccessModeResult;
import com.emc.vipr.services.s3.model.GetFileAccessRequest;
import com.emc.vipr.services.s3.model.GetFileAccessResult;
import com.emc.vipr.services.s3.model.PutAccessModeRequest;
import com.emc.vipr.services.s3.model.PutAccessModeResult;
import com.emc.vipr.services.s3.model.UpdateObjectRequest;
import com.emc.vipr.services.s3.model.UpdateObjectResult;

/**
 * @author cwikj
 *
 */
public class ViPRS3Client implements ViPRS3, AmazonS3 {
	private AmazonS3Client delegate;
	private ViPRRequestHandler requestHandler;
	
	/** 
	 * Since the S3 RequestHandlers are stored in a private List, we can't ensure that multiple
	 * ViPRS3Clients haven't registered handlers with the AmazonS3Client.  We will use this Thread
	 * Local to keep track of the currently executing ViPRS3Client so that any extraneous 
	 * RequestHandlers will ignore the request if it's not from the currently executing
	 * ViPRS3Client.
	 */
	private static ThreadLocal<ViPRS3Client> currentClient = new ThreadLocal<ViPRS3Client>();

	public ViPRS3Client(AmazonS3Client delegate) {
		this.delegate = delegate;
		this.requestHandler = new ViPRRequestHandler();

		// Register the handler
		delegate.addRequestHandler(requestHandler);
	}
	
	@Override
	protected void finalize() throws Throwable {
		// Unregister our delegate
		delegate.removeRequestHandler(requestHandler);
		super.finalize();
	}

	/**
	 * @see com.amazonaws.services.s3.AmazonS3#setEndpoint(java.lang.String)
	 */
	@Override
	public void setEndpoint(String endpoint) {
		delegate.setEndpoint(endpoint);
	}

	/**
	 * @see com.amazonaws.services.s3.AmazonS3#setRegion(com.amazonaws.regions.Region)
	 */
	@Override
	public void setRegion(Region region) throws IllegalArgumentException {
		delegate.setRegion(region);
	}

	/**
	 * @see com.amazonaws.services.s3.AmazonS3#setS3ClientOptions(com.amazonaws.services.s3.S3ClientOptions)
	 */
	@Override
	public void setS3ClientOptions(S3ClientOptions clientOptions) {
		delegate.setS3ClientOptions(clientOptions);
	}

	/**
	 * @see com.amazonaws.services.s3.AmazonS3#changeObjectStorageClass(java.lang.String, java.lang.String, com.amazonaws.services.s3.model.StorageClass)
	 */
	@Override
	public void changeObjectStorageClass(String bucketName, String key,
			StorageClass newStorageClass) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		delegate.changeObjectStorageClass(bucketName, key, newStorageClass);
		unsetClient();
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectRedirectLocation(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void setObjectRedirectLocation(String bucketName, String key,
			String newRedirectLocation) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listObjects(java.lang.String)
	 */
	@Override
	public ObjectListing listObjects(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listObjects(java.lang.String, java.lang.String)
	 */
	@Override
	public ObjectListing listObjects(String bucketName, String prefix)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest)
	 */
	@Override
	public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listNextBatchOfObjects(com.amazonaws.services.s3.model.ObjectListing)
	 */
	@Override
	public ObjectListing listNextBatchOfObjects(
			ObjectListing previousObjectListing) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listVersions(java.lang.String, java.lang.String)
	 */
	@Override
	public VersionListing listVersions(String bucketName, String prefix)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listNextBatchOfVersions(com.amazonaws.services.s3.model.VersionListing)
	 */
	@Override
	public VersionListing listNextBatchOfVersions(
			VersionListing previousVersionListing)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listVersions(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.Integer)
	 */
	@Override
	public VersionListing listVersions(String bucketName, String prefix,
			String keyMarker, String versionIdMarker, String delimiter,
			Integer maxResults) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest)
	 */
	@Override
	public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getS3AccountOwner()
	 */
	@Override
	public Owner getS3AccountOwner() throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#doesBucketExist(java.lang.String)
	 */
	@Override
	public boolean doesBucketExist(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return false;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listBuckets()
	 */
	@Override
	public List<Bucket> listBuckets() throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listBuckets(com.amazonaws.services.s3.model.ListBucketsRequest)
	 */
	@Override
	public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLocation(java.lang.String)
	 */
	@Override
	public String getBucketLocation(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLocation(com.amazonaws.services.s3.model.GetBucketLocationRequest)
	 */
	@Override
	public String getBucketLocation(
			GetBucketLocationRequest getBucketLocationRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest)
	 */
	@Override
	public Bucket createBucket(CreateBucketRequest createBucketRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(java.lang.String)
	 */
	@Override
	public Bucket createBucket(String bucketName) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(java.lang.String, com.amazonaws.services.s3.model.Region)
	 */
	@Override
	public Bucket createBucket(String bucketName,
			com.amazonaws.services.s3.model.Region region)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(java.lang.String, java.lang.String)
	 */
	@Override
	public Bucket createBucket(String bucketName, String region)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectAcl(java.lang.String, java.lang.String)
	 */
	@Override
	public AccessControlList getObjectAcl(String bucketName, String key)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectAcl(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public AccessControlList getObjectAcl(String bucketName, String key,
			String versionId) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, com.amazonaws.services.s3.model.AccessControlList)
	 */
	@Override
	public void setObjectAcl(String bucketName, String key,
			AccessControlList acl) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, com.amazonaws.services.s3.model.CannedAccessControlList)
	 */
	@Override
	public void setObjectAcl(String bucketName, String key,
			CannedAccessControlList acl) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, java.lang.String, com.amazonaws.services.s3.model.AccessControlList)
	 */
	@Override
	public void setObjectAcl(String bucketName, String key, String versionId,
			AccessControlList acl) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, java.lang.String, com.amazonaws.services.s3.model.CannedAccessControlList)
	 */
	@Override
	public void setObjectAcl(String bucketName, String key, String versionId,
			CannedAccessControlList acl) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketAcl(java.lang.String)
	 */
	@Override
	public AccessControlList getBucketAcl(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketAcl(com.amazonaws.services.s3.model.SetBucketAclRequest)
	 */
	@Override
	public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketAcl(com.amazonaws.services.s3.model.GetBucketAclRequest)
	 */
	@Override
	public AccessControlList getBucketAcl(
			GetBucketAclRequest getBucketAclRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketAcl(java.lang.String, com.amazonaws.services.s3.model.AccessControlList)
	 */
	@Override
	public void setBucketAcl(String bucketName, AccessControlList acl)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketAcl(java.lang.String, com.amazonaws.services.s3.model.CannedAccessControlList)
	 */
	@Override
	public void setBucketAcl(String bucketName, CannedAccessControlList acl)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectMetadata(java.lang.String, java.lang.String)
	 */
	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String key)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectMetadata(com.amazonaws.services.s3.model.GetObjectMetadataRequest)
	 */
	@Override
	public ObjectMetadata getObjectMetadata(
			GetObjectMetadataRequest getObjectMetadataRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getObject(java.lang.String, java.lang.String)
	 */
	@Override
	public S3Object getObject(String bucketName, String key)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getObject(com.amazonaws.services.s3.model.GetObjectRequest)
	 */
	@Override
	public S3Object getObject(GetObjectRequest getObjectRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getObject(com.amazonaws.services.s3.model.GetObjectRequest, java.io.File)
	 */
	@Override
	public ObjectMetadata getObject(GetObjectRequest getObjectRequest,
			File destinationFile) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucket(com.amazonaws.services.s3.model.DeleteBucketRequest)
	 */
	@Override
	public void deleteBucket(DeleteBucketRequest deleteBucketRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucket(java.lang.String)
	 */
	@Override
	public void deleteBucket(String bucketName) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#putObject(com.amazonaws.services.s3.model.PutObjectRequest)
	 */
	@Override
	public PutObjectResult putObject(PutObjectRequest putObjectRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#putObject(java.lang.String, java.lang.String, java.io.File)
	 */
	@Override
	public PutObjectResult putObject(String bucketName, String key, File file)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#putObject(java.lang.String, java.lang.String, java.io.InputStream, com.amazonaws.services.s3.model.ObjectMetadata)
	 */
	@Override
	public PutObjectResult putObject(String bucketName, String key,
			InputStream input, ObjectMetadata metadata)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#copyObject(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public CopyObjectResult copyObject(String sourceBucketName,
			String sourceKey, String destinationBucketName,
			String destinationKey) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#copyObject(com.amazonaws.services.s3.model.CopyObjectRequest)
	 */
	@Override
	public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#copyPart(com.amazonaws.services.s3.model.CopyPartRequest)
	 */
	@Override
	public CopyPartResult copyPart(CopyPartRequest copyPartRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteObject(java.lang.String, java.lang.String)
	 */
	@Override
	public void deleteObject(String bucketName, String key)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteObject(com.amazonaws.services.s3.model.DeleteObjectRequest)
	 */
	@Override
	public void deleteObject(DeleteObjectRequest deleteObjectRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteObjects(com.amazonaws.services.s3.model.DeleteObjectsRequest)
	 */
	@Override
	public DeleteObjectsResult deleteObjects(
			DeleteObjectsRequest deleteObjectsRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteVersion(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void deleteVersion(String bucketName, String key, String versionId)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteVersion(com.amazonaws.services.s3.model.DeleteVersionRequest)
	 */
	@Override
	public void deleteVersion(DeleteVersionRequest deleteVersionRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLoggingConfiguration(java.lang.String)
	 */
	@Override
	public BucketLoggingConfiguration getBucketLoggingConfiguration(
			String bucketName) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketLoggingConfiguration(com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest)
	 */
	@Override
	public void setBucketLoggingConfiguration(
			SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketVersioningConfiguration(java.lang.String)
	 */
	@Override
	public BucketVersioningConfiguration getBucketVersioningConfiguration(
			String bucketName) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketVersioningConfiguration(com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest)
	 */
	@Override
	public void setBucketVersioningConfiguration(
			SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLifecycleConfiguration(java.lang.String)
	 */
	@Override
	public BucketLifecycleConfiguration getBucketLifecycleConfiguration(
			String bucketName) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketLifecycleConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketLifecycleConfiguration)
	 */
	@Override
	public void setBucketLifecycleConfiguration(String bucketName,
			BucketLifecycleConfiguration bucketLifecycleConfiguration) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketLifecycleConfiguration(java.lang.String)
	 */
	@Override
	public void deleteBucketLifecycleConfiguration(String bucketName) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketCrossOriginConfiguration(java.lang.String)
	 */
	@Override
	public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(
			String bucketName) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketCrossOriginConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketCrossOriginConfiguration)
	 */
	@Override
	public void setBucketCrossOriginConfiguration(String bucketName,
			BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketCrossOriginConfiguration(java.lang.String)
	 */
	@Override
	public void deleteBucketCrossOriginConfiguration(String bucketName) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketTaggingConfiguration(java.lang.String)
	 */
	@Override
	public BucketTaggingConfiguration getBucketTaggingConfiguration(
			String bucketName) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketTaggingConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketTaggingConfiguration)
	 */
	@Override
	public void setBucketTaggingConfiguration(String bucketName,
			BucketTaggingConfiguration bucketTaggingConfiguration) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketTaggingConfiguration(java.lang.String)
	 */
	@Override
	public void deleteBucketTaggingConfiguration(String bucketName) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketNotificationConfiguration(java.lang.String)
	 */
	@Override
	public BucketNotificationConfiguration getBucketNotificationConfiguration(
			String bucketName) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketNotificationConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketNotificationConfiguration)
	 */
	@Override
	public void setBucketNotificationConfiguration(String bucketName,
			BucketNotificationConfiguration bucketNotificationConfiguration)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketWebsiteConfiguration(java.lang.String)
	 */
	@Override
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
			String bucketName) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketWebsiteConfiguration(com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest)
	 */
	@Override
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
			GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketWebsiteConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketWebsiteConfiguration)
	 */
	@Override
	public void setBucketWebsiteConfiguration(String bucketName,
			BucketWebsiteConfiguration configuration)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketWebsiteConfiguration(com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest)
	 */
	@Override
	public void setBucketWebsiteConfiguration(
			SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketWebsiteConfiguration(java.lang.String)
	 */
	@Override
	public void deleteBucketWebsiteConfiguration(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketWebsiteConfiguration(com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest)
	 */
	@Override
	public void deleteBucketWebsiteConfiguration(
			DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketPolicy(java.lang.String)
	 */
	@Override
	public BucketPolicy getBucketPolicy(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketPolicy(com.amazonaws.services.s3.model.GetBucketPolicyRequest)
	 */
	@Override
	public BucketPolicy getBucketPolicy(
			GetBucketPolicyRequest getBucketPolicyRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketPolicy(java.lang.String, java.lang.String)
	 */
	@Override
	public void setBucketPolicy(String bucketName, String policyText)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketPolicy(com.amazonaws.services.s3.model.SetBucketPolicyRequest)
	 */
	@Override
	public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketPolicy(java.lang.String)
	 */
	@Override
	public void deleteBucketPolicy(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketPolicy(com.amazonaws.services.s3.model.DeleteBucketPolicyRequest)
	 */
	@Override
	public void deleteBucketPolicy(
			DeleteBucketPolicyRequest deleteBucketPolicyRequest)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#generatePresignedUrl(java.lang.String, java.lang.String, java.util.Date)
	 */
	@Override
	public URL generatePresignedUrl(String bucketName, String key,
			Date expiration) throws AmazonClientException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#generatePresignedUrl(java.lang.String, java.lang.String, java.util.Date, com.amazonaws.HttpMethod)
	 */
	@Override
	public URL generatePresignedUrl(String bucketName, String key,
			Date expiration, HttpMethod method) throws AmazonClientException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#generatePresignedUrl(com.amazonaws.services.s3.model.GeneratePresignedUrlRequest)
	 */
	@Override
	public URL generatePresignedUrl(
			GeneratePresignedUrlRequest generatePresignedUrlRequest)
			throws AmazonClientException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#initiateMultipartUpload(com.amazonaws.services.s3.model.InitiateMultipartUploadRequest)
	 */
	@Override
	public InitiateMultipartUploadResult initiateMultipartUpload(
			InitiateMultipartUploadRequest request)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#uploadPart(com.amazonaws.services.s3.model.UploadPartRequest)
	 */
	@Override
	public UploadPartResult uploadPart(UploadPartRequest request)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listParts(com.amazonaws.services.s3.model.ListPartsRequest)
	 */
	@Override
	public PartListing listParts(ListPartsRequest request)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#abortMultipartUpload(com.amazonaws.services.s3.model.AbortMultipartUploadRequest)
	 */
	@Override
	public void abortMultipartUpload(AbortMultipartUploadRequest request)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#completeMultipartUpload(com.amazonaws.services.s3.model.CompleteMultipartUploadRequest)
	 */
	@Override
	public CompleteMultipartUploadResult completeMultipartUpload(
			CompleteMultipartUploadRequest request)
			throws AmazonClientException, AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#listMultipartUploads(com.amazonaws.services.s3.model.ListMultipartUploadsRequest)
	 */
	@Override
	public MultipartUploadListing listMultipartUploads(
			ListMultipartUploadsRequest request) throws AmazonClientException,
			AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#getCachedResponseMetadata(com.amazonaws.AmazonWebServiceRequest)
	 */
	@Override
	public S3ResponseMetadata getCachedResponseMetadata(
			AmazonWebServiceRequest request) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#restoreObject(com.amazonaws.services.s3.model.RestoreObjectRequest)
	 */
	@Override
	public void restoreObject(RestoreObjectRequest copyGlacierObjectRequest)
			throws AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}

	/* (non-Javadoc)
	 * @see com.amazonaws.services.s3.AmazonS3#restoreObject(java.lang.String, java.lang.String, int)
	 */
	@Override
	public void restoreObject(String bucketName, String key,
			int expirationInDays) throws AmazonServiceException {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();

	}
	
	@Override
	public UpdateObjectResult updateObject(String bucketName, String key,
			File file, long startOffset) throws AmazonClientException,
			AmazonServiceException {
		UpdateObjectRequest request = new UpdateObjectRequest(bucketName, key, file).withUpdateOffset(startOffset);
		
		return updateObject(request);
	}

	@Override
	public UpdateObjectResult updateObject(String bucketName, String key,
			InputStream input, ObjectMetadata metadata, long startOffset)
			throws AmazonClientException, AmazonServiceException {
		UpdateObjectRequest request = new UpdateObjectRequest(bucketName, key, input, metadata).withUpdateOffset(startOffset);
		
		return updateObject(request);
	}
	
	@Override
	public UpdateObjectResult updateObject(UpdateObjectRequest request) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}


	@Override
	public AppendObjectResult appendObject(String bucketName, String key,
			File file) throws AmazonClientException, AmazonServiceException {
		AppendObjectRequest request = new AppendObjectRequest(bucketName, key, file);
		
		return appendObject(request);
	}

	@Override
	public AppendObjectResult appendObject(String bucketName, String key,
			InputStream input, ObjectMetadata metadata)
			throws AmazonClientException, AmazonServiceException {
		AppendObjectRequest request = new AppendObjectRequest(bucketName, key, input, metadata);
		
		return appendObject(request);
	}
	
	@Override
	public AppendObjectResult appendObject(AppendObjectRequest request) {
		setClient();
		// TODO Auto-generated method stub
		unsetClient();
		return null;
	}

	@Override
	public PutAccessModeResult putAccessMode(PutAccessModeRequest request)
			throws AmazonClientException, AmazonServiceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GetAccessModeResult getAccessMode(String bucketName)
			throws AmazonClientException, AmazonServiceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GetFileAccessResult getFileAccess(GetFileAccessRequest request)
			throws AmazonClientException, AmazonServiceException {
		// TODO Auto-generated method stub
		return null;
	}

	
	private void setClient() {
		currentClient.set(this);
	}
	
	private void unsetClient() {
		currentClient.set(null);
	}
	
	public class ViPRRequestHandler implements RequestHandler {

		@Override
		public void beforeRequest(Request<?> request) {
			if(ViPRS3Client.this != currentClient.get()) {
				// Not currently executing client.
				return;
			}
			
		}

		@Override
		public void afterResponse(Request<?> request, Object response,
				TimingInfo timingInfo) {
			if(ViPRS3Client.this != currentClient.get()) {
				// Not currently executing client.
				return;
			}
			
		}

		@Override
		public void afterError(Request<?> request, Exception e) {
			if(ViPRS3Client.this != currentClient.get()) {
				// Not currently executing client.
				return;
			}
			
		}
		
	}
}
