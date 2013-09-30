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
package com.emc.vipr.sample;

import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.emc.vipr.services.s3.ViPRS3Client;

public class Namespace {
    /**
     * If you're using ViPR namespaces other than the default, then you should
     * use the ViPRS3Client implementation and
     * call setNamespace(). If you wish to use an
     * encryption client like AmazonS3EncryptionClient, which does not have a
     * counterpart in the ViPR SDK, then you must use the default namespace. Here are examples of each.
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

	// This is how you instantiate the ViPRS3Client.
	BasicAWSCredentials creds = new BasicAWSCredentials(accessKey,
		secretKey);
	ViPRS3Client client = new ViPRS3Client(endpoint, creds);
	client.setNamespace(namespace);

    // If you aren't going to use DNS-style namespaces and buckets (they are currently tied
    // together), do the following:
    S3ClientOptions clientOptions = new S3ClientOptions();
    clientOptions.setPathStyleAccess(true);
    client.setS3ClientOptions(clientOptions);

	// This is how you instantiate the encryption client. you can only use the default namespace with this client.
	EncryptionMaterials keys = null; // defining the keys is left to you

	creds = new BasicAWSCredentials(accessKey, secretKey);
	AmazonS3EncryptionClient client2 = new AmazonS3EncryptionClient(creds,
		keys);
	client2.setEndpoint(endpoint);

	// that's it!
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
