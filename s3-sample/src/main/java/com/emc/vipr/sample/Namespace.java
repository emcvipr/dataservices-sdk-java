package com.emc.vipr.sample;

import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.emc.vipr.services.s3.NamespaceRequestHandler;
import com.emc.vipr.services.s3.ViPRS3Client;

public class Namespace {
    /**
     * If you're using explicit ViPR namespaces (by not using DNS-specified
     * namespaces), then you should just use the ViPRS3Client implementation and
     * call setNamespace(). The only exception to this is if you wish to use an
     * encryption client, like AmazonS3EncryptionClient, which does not have a
     * counterpart in the ViPR SDK. Here are examples of each.
     * 
     * @throws Exception
     */
    public void runSample() throws Exception {
	Properties props = S3ClientFactory.getProperties();

	String accessKey = props
		.getProperty(S3ClientFactory.PROP_ACCESS_KEY_ID);
	String secretKey = props.getProperty(S3ClientFactory.PROP_SECRET_KEY);
	String endpoint = props.getProperty(S3ClientFactory.PROP_ENDPOINT);
	String namespace = props.getProperty(S3ClientFactory.PROP_NAMESPACE);

	// This is how you instantiate the ViPRS3Client. Note that you only
	// need to set the namespace if you are *not* specifying the namespace
	// via DNS hostname.
	BasicAWSCredentials creds = new BasicAWSCredentials(accessKey,
		secretKey);
	ViPRS3Client client = new ViPRS3Client(creds);
	client.setEndpoint(endpoint);
	client.setNamespace(namespace);

	// This is how you instantiate the encryption client using the
	// namespace handler. Again, only use the namespace handler if you are
	// *not* using DNS-based namespace.
	EncryptionMaterials keys = null; // defining the keys is left to you

	creds = new BasicAWSCredentials(accessKey, secretKey);
	AmazonS3EncryptionClient client2 = new AmazonS3EncryptionClient(creds,
		keys);
	client2.setEndpoint(endpoint);
	client2.addRequestHandler(new NamespaceRequestHandler(namespace));
	
	// that's it!  Now, all of your requests will have an explicit namespace
	// specified as a header.
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	Namespace instance = new Namespace();
	instance.runSample();
    }
}
