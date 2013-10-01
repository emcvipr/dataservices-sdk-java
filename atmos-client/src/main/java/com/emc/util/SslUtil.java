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
package com.emc.util;

import javax.net.ssl.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SslUtil {

    public static TrustManager[] gullibleManagers = new TrustManager[]{
            new X509TrustManager() {
                @Override
                public void checkClientTrusted( X509Certificate[] x509Certificates, String s ) throws CertificateException {
                }

                @Override
                public void checkServerTrusted( X509Certificate[] x509Certificates, String s ) throws CertificateException {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            }
    };

    public static HostnameVerifier gullibleVerifier = new HostnameVerifier() {
        @Override
        public boolean verify( String s, SSLSession sslSession ) {
            return true;
        }
    };

    public static SSLContext createGullibleSslContext() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext ctx = SSLContext.getInstance( "SSL" );
        ctx.init( null, gullibleManagers, new SecureRandom() );
        return ctx;
    }
}
