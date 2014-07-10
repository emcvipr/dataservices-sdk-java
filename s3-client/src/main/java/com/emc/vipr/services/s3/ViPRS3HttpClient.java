/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.http.AmazonHttpClient;
import com.emc.vipr.ribbon.SmartHttpClient;
import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.LoadBalancerStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.*;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.ChallengeState;
import org.apache.http.auth.NTCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.*;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class ViPRS3HttpClient extends AmazonHttpClient {
    private static Log log = LogFactory.getLog(ViPRS3HttpClient.class);

    public ViPRS3HttpClient(ViPRS3Config viprConfig) {
        super(viprConfig.getClientConfiguration(), new SmartHttpClient(viprConfig.toSmartClientConfig()), null);

        ClientConfiguration azConfig = viprConfig.getClientConfiguration();
        HttpParams httpClientParams = httpClient.getParams();

        HttpConnectionParams.setConnectionTimeout(httpClientParams, azConfig.getConnectionTimeout());
        HttpConnectionParams.setSoTimeout(httpClientParams, azConfig.getSocketTimeout());
        HttpConnectionParams.setStaleCheckingEnabled(httpClientParams, true);
        HttpConnectionParams.setTcpNoDelay(httpClientParams, true);

        int socketSendBufferSizeHint = azConfig.getSocketBufferSizeHints()[0];
        int socketReceiveBufferSizeHint = azConfig.getSocketBufferSizeHints()[1];
        if (socketSendBufferSizeHint > 0 || socketReceiveBufferSizeHint > 0) {
            HttpConnectionParams.setSocketBufferSize(httpClientParams,
                    Math.max(socketSendBufferSizeHint, socketReceiveBufferSizeHint));
        }

        ClientConnectionManager connectionManager = httpClient.getConnectionManager();
        ((SmartHttpClient) httpClient).setRedirectStrategy(new LocationHeaderNotRequiredRedirectStrategy());

        try {
            Scheme http = new Scheme("http", 80, PlainSocketFactory.getSocketFactory());
            SSLSocketFactory sf = new SSLSocketFactory(
                    SSLContext.getDefault(),
                    SSLSocketFactory.STRICT_HOSTNAME_VERIFIER);
            Scheme https = new Scheme("https", 443, sf);
            SchemeRegistry sr = connectionManager.getSchemeRegistry();
            sr.register(http);
            sr.register(https);
        } catch (NoSuchAlgorithmException e) {
            throw new AmazonClientException("Unable to access default SSL context", e);
        }

        /*
         * If SSL cert checking for endpoints has been explicitly disabled,
         * register a new scheme for HTTPS that won't cause self-signed certs to
         * error out.
         */
        if (System.getProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY) != null) {
            Scheme sch = new Scheme("https", 443, new TrustingSocketFactory());
            httpClient.getConnectionManager().getSchemeRegistry().register(sch);
        }

        /* Set proxy if configured */
        String proxyHost = azConfig.getProxyHost();
        int proxyPort = azConfig.getProxyPort();
        if (proxyHost != null && proxyPort > 0) {
            log.info("Configuring Proxy. Proxy Host: " + proxyHost + " " + "Proxy Port: " + proxyPort);
            HttpHost proxyHttpHost = new HttpHost(proxyHost, proxyPort);
            httpClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxyHttpHost);

            String proxyUsername = azConfig.getProxyUsername();
            String proxyPassword = azConfig.getProxyPassword();
            String proxyDomain = azConfig.getProxyDomain();
            String proxyWorkstation = azConfig.getProxyWorkstation();

            if (proxyUsername != null && proxyPassword != null) {
                ((SmartHttpClient) httpClient).getCredentialsProvider().setCredentials(
                        new AuthScope(proxyHost, proxyPort),
                        new NTCredentials(proxyUsername, proxyPassword, proxyWorkstation, proxyDomain));
            }

            // Add a request interceptor that sets up proxy authentication pre-emptively if configured
            if (azConfig.isPreemptiveBasicProxyAuth()) {
                ((SmartHttpClient) httpClient).addRequestInterceptor(new PreemptiveProxyAuth(proxyHttpHost), 0);
            }
        }
    }

    public LoadBalancerStats getLoadBalancerStats() {
        return ((BaseLoadBalancer) ((SmartHttpClient) httpClient).getLoadBalancer()).getLoadBalancerStats();
    }

    /**
     * Customization of the default redirect strategy provided by HttpClient to be a little
     * less strict about the Location header to account for S3 not sending the Location
     * header with 301 responses.
     */
    private static final class LocationHeaderNotRequiredRedirectStrategy
            extends DefaultRedirectStrategy {

        @Override
        public boolean isRedirected(HttpRequest request,
                                    HttpResponse response, HttpContext context) throws ProtocolException {
            int statusCode = response.getStatusLine().getStatusCode();
            Header locationHeader = response.getFirstHeader("location");

            // Instead of throwing a ProtocolException in this case, just
            // return false to indicate that this is not redirected
            if (locationHeader == null &&
                    statusCode == HttpStatus.SC_MOVED_PERMANENTLY) return false;

            return super.isRedirected(request, response, context);
        }
    }

    /**
     * Simple implementation of SchemeSocketFactory (and
     * LayeredSchemeSocketFactory) that bypasses SSL certificate checks. This
     * class is only intended to be used for testing purposes.
     */
    private static class TrustingSocketFactory implements SchemeSocketFactory, SchemeLayeredSocketFactory {

        private SSLContext sslcontext = null;

        private static SSLContext createSSLContext() throws IOException {
            try {
                SSLContext context = SSLContext.getInstance("TLS");
                context.init(null, new TrustManager[]{new TrustingX509TrustManager()}, null);
                return context;
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        private SSLContext getSSLContext() throws IOException {
            if (this.sslcontext == null) this.sslcontext = createSSLContext();
            return this.sslcontext;
        }

        public Socket createSocket(HttpParams params) throws IOException {
            return getSSLContext().getSocketFactory().createSocket();
        }

        public Socket connectSocket(Socket sock,
                                    InetSocketAddress remoteAddress,
                                    InetSocketAddress localAddress, HttpParams params)
                throws IOException, UnknownHostException,
                ConnectTimeoutException {
            int connTimeout = HttpConnectionParams.getConnectionTimeout(params);
            int soTimeout = HttpConnectionParams.getSoTimeout(params);

            SSLSocket sslsock = (SSLSocket) ((sock != null) ? sock : createSocket(params));
            if (localAddress != null) sslsock.bind(localAddress);

            sslsock.connect(remoteAddress, connTimeout);
            sslsock.setSoTimeout(soTimeout);
            return sslsock;
        }

        public boolean isSecure(Socket sock) throws IllegalArgumentException {
            return true;
        }

        public Socket createLayeredSocket(Socket arg0, String arg1, int arg2, HttpParams arg3)
                throws IOException, UnknownHostException {
            return getSSLContext().getSocketFactory().createSocket(arg0, arg1, arg2, true);
        }
    }

    /**
     * Simple implementation of X509TrustManager that trusts all certificates.
     * This class is only intended to be used for testing purposes.
     */
    private static class TrustingX509TrustManager implements X509TrustManager {
        private static final X509Certificate[] X509_CERTIFICATES = new X509Certificate[0];

        public X509Certificate[] getAcceptedIssuers() {
            return X509_CERTIFICATES;
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            // No-op, to trust all certs
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            // No-op, to trust all certs
        }
    }

    /**
     * HttpRequestInterceptor implementation to set up pre-emptive
     * authentication against a defined basic proxy server.
     */
    private static class PreemptiveProxyAuth implements HttpRequestInterceptor {
        private final HttpHost proxyHost;

        public PreemptiveProxyAuth(HttpHost proxyHost) {
            this.proxyHost = proxyHost;
        }

        public void process(HttpRequest request, HttpContext context) {
            AuthCache authCache;
            // Set up the a Basic Auth scheme scoped for the proxy - we don't
            // want to do this for non-proxy authentication.
            BasicScheme basicScheme = new BasicScheme(ChallengeState.PROXY);

            if (context.getAttribute(ClientContext.AUTH_CACHE) == null) {
                authCache = new BasicAuthCache();
                authCache.put(this.proxyHost, basicScheme);
                context.setAttribute(ClientContext.AUTH_CACHE, authCache);
            } else {
                authCache =
                        (AuthCache) context.getAttribute(ClientContext.AUTH_CACHE);
                authCache.put(this.proxyHost, basicScheme);
            }
        }
    }
}
