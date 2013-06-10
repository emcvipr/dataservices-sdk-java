/**
 * 
 */
package com.emc.vipr.services.s3;

import com.amazonaws.Request;
import com.amazonaws.handlers.RequestHandler;
import com.amazonaws.util.TimingInfo;
import com.emc.vipr.services.s3.model.ViPRConstants;

/**
 * This handler can be added to an AmazonS3Client class to override
 * the ViPR namespace.  Generally, this will be
 * automatically determined by the endpoint of the underlying
 * S3 client in the form of {namespace}.company.com, but if this
 * is not possible, it can be manually overriden by creating an instance
 * of this class and registering it with the client.  Note that the
 * AmazonS3Client does not have a way to check if a handler is
 * already registered, so care must be taken to ensure that handlers
 * only get added once.
 */
public class NamespaceRequestHandler implements RequestHandler {
	private String namespace;
	
	/**
	 * Constructs a new NamespaceRequestHandler.
	 * @param namespace the namespace to inject into requests.
	 */
	public NamespaceRequestHandler(String namespace) {
		this.namespace = namespace;
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.handlers.RequestHandler#beforeRequest(com.amazonaws.Request)
	 */
	public void beforeRequest(Request<?> request) {
		request.addHeader(ViPRConstants.NAMESPACE_HEADER, namespace);
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.handlers.RequestHandler#afterResponse(com.amazonaws.Request, java.lang.Object, com.amazonaws.util.TimingInfo)
	 */
	public void afterResponse(Request<?> request, Object response,
			TimingInfo timingInfo) {
	}

	/* (non-Javadoc)
	 * @see com.amazonaws.handlers.RequestHandler#afterError(com.amazonaws.Request, java.lang.Exception)
	 */
	public void afterError(Request<?> request, Exception e) {
	}

}
