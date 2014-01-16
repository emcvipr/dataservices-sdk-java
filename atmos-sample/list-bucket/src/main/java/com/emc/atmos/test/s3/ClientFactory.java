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
package com.emc.atmos.test.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * This class looks on the classpath for a file named atmos.properties and uses it to
 * configure a connection to Atmos.  The supported properties are:
 * <dt>
 * <dl>atmos.uid</dl><dd>(Required) The full token ID</dd>
 * <dl>atmos.secret</dl><dd>(Required) The shared secret key</dd>
 * <dl>atmos.endpoints</dl><dd>(Required) Comma-delimited list of endpoint URIs</dd>
 * <dl>s3.endpoint</dl><dd>(Required) S3 endpoint URI</dd>
 * <dl>atmos.proxyUrl</dl><dd>(Optional) proxy to use</dd>
 * </dt>
 *
 * @author cwikj
 */
public class ClientFactory {
    public static final String ATMOS_PROPERTIES_FILE = "atmos.properties";

    public static final String PROP_UID = "atmos.uid";
    public static final String PROP_SECRET = "atmos.secret";
    public static final String PROP_S3_ENDPOINT = "s3.endpoint";
    public static final String PROP_PROXY = "atmos.proxyUrl";

    private static Properties properties;

    /**
     * Locates and loads the properties file for the test configuration.  This file can
     * reside in one of two places: somewhere in the CLASSPATH or in the user's home
     * directory.
     *
     * @return the contents of the properties file as a {@link java.util.Properties} object.
     * @throws java.io.FileNotFoundException if the file was not found
     * @throws java.io.IOException           if there was an error reading the file.
     */
    public static synchronized Properties getProperties() throws IOException {
        if (properties != null) return properties;

        InputStream in = ClientFactory.class.getClassLoader().getResourceAsStream(ATMOS_PROPERTIES_FILE);
        if (in == null) {
            // Check in home directory
            File homeProps = new File(System.getProperty("user.home") + File.separator + ATMOS_PROPERTIES_FILE);
            if (homeProps.exists()) {
                in = new FileInputStream(homeProps);
            } else {
                throw new FileNotFoundException(ATMOS_PROPERTIES_FILE);
            }
        }

        properties = new Properties();
        properties.load(in);
        in.close();

        return properties;
    }

    public static AmazonS3 getS3Client() {
        try {
            Properties props = getProperties();

            String uid = getPropertyNotEmpty(props, PROP_UID);
            String secret = getPropertyNotEmpty(props, PROP_SECRET);
            String endpoint = getPropertyNotEmpty(props, PROP_S3_ENDPOINT);
            String proxyUrl = props.getProperty(PROP_PROXY);

            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.valueOf(new URI(endpoint).getScheme().toUpperCase()));
            if (proxyUrl != null) {
                URI proxyUri = new URI(proxyUrl);
                config.setProxyHost(proxyUri.getHost());
                config.setProxyPort(proxyUri.getPort());
            }

            AmazonS3Client s3 = new AmazonS3Client(new BasicAWSCredentials(uid, secret), config);
            s3.setEndpoint(endpoint);

            S3ClientOptions options = new S3ClientOptions();
            options.setPathStyleAccess(true);
            s3.setS3ClientOptions(options);

            return s3;
        } catch (IOException e) {
            throw new RuntimeException("Could not load properties file", e);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid endpoint or proxy URI", e);
        }
    }

    private static String getPropertyNotEmpty(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new RuntimeException(String.format("The property %s is required", key));
        }
        return value;
    }
}
