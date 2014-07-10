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
package com.emc.vipr.ribbon;

import com.netflix.client.*;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.http4.NFHttpClientFactory;
import com.netflix.loadbalancer.ILoadBalancer;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.RequestWrapper;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartHttpClient implements HttpClient {
    private static final String CLIENT_NAME = "ViPR.SmartHttpClient";
    private static final AtomicInteger clientCount = new AtomicInteger();

    protected AbstractHttpClient delegateClient;

    protected SmartLoadBalancer loadBalancer;

    public SmartHttpClient(SmartClientConfig smartConfig) {
        this(null, smartConfig);
    }

    public SmartHttpClient(String name) {
        this(name, null);
    }

    public SmartHttpClient(String name, SmartClientConfig smartConfig) {
        if (name == null) name = CLIENT_NAME + '_' + clientCount.incrementAndGet();

        // Initialize config properties
        IClientConfig nfConfig = ClientFactory.getNamedConfig(name);
        initConfig(nfConfig, smartConfig);

        // Pull Ribbon's HttpClient instance (it collects metrics for the LB). This will pull our initialized config by
        // name
        this.delegateClient = NFHttpClientFactory.getNamedNFHttpClient(name, nfConfig);

        // Pull the "client" (only used for load balancing)
        // This is unusual but effective since it performs all of the necessary Ribbon initialization for load balancing
        // without actually creating a client. This also pulls our initialized config by name
        this.loadBalancer = (SmartLoadBalancer) ClientFactory.getNamedClient(name);
    }

    @Override
    public HttpParams getParams() {
        return delegateClient.getParams();
    }

    @Override
    public ClientConnectionManager getConnectionManager() {
        return delegateClient.getConnectionManager();
    }

    @Override
    public HttpResponse execute(final HttpUriRequest request) throws IOException {
        return loadBalancer.executeWhenResolved(request.getURI(), new SmartLoadBalancer.ResolvedExecution<HttpResponse>() {
            @Override
            public HttpResponse executeResolved(URI resolvedUri) throws IOException {
                setUri(request, resolvedUri);
                return delegateClient.execute(request);
            }
        });
    }

    @Override
    public HttpResponse execute(final HttpUriRequest request, final HttpContext context) throws IOException {
        return loadBalancer.executeWhenResolved(request.getURI(), new SmartLoadBalancer.ResolvedExecution<HttpResponse>() {
            @Override
            public HttpResponse executeResolved(URI resolvedUri) throws IOException {
                setUri(request, resolvedUri);
                return delegateClient.execute(request, context);
            }
        });
    }

    @Override
    public HttpResponse execute(final HttpHost target, final HttpRequest request) throws IOException {
        return loadBalancer.executeWhenResolved(toUri(target), new SmartLoadBalancer.ResolvedExecution<HttpResponse>() {
            @Override
            public HttpResponse executeResolved(URI resolvedUri) throws IOException {
                return delegateClient.execute(fromUri(resolvedUri), request);
            }
        });
    }

    @Override
    public HttpResponse execute(final HttpHost target, final HttpRequest request, final HttpContext context) throws IOException {
        return loadBalancer.executeWhenResolved(toUri(target), new SmartLoadBalancer.ResolvedExecution<HttpResponse>() {
            @Override
            public HttpResponse executeResolved(URI resolvedUri) throws IOException {
                return delegateClient.execute(fromUri(resolvedUri), request, context);
            }
        });
    }

    @Override
    public <T> T execute(final HttpUriRequest request, final ResponseHandler<? extends T> responseHandler) throws IOException {
        return loadBalancer.executeWhenResolved(request.getURI(), new SmartLoadBalancer.ResolvedExecution<T>() {
            @Override
            public T executeResolved(URI resolvedUri) throws IOException {
                setUri(request, resolvedUri);
                return delegateClient.execute(request, responseHandler);
            }
        });
    }

    @Override
    public <T> T execute(final HttpUriRequest request, final ResponseHandler<? extends T> responseHandler, final HttpContext context) throws IOException {
        return loadBalancer.executeWhenResolved(request.getURI(), new SmartLoadBalancer.ResolvedExecution<T>() {
            @Override
            public T executeResolved(URI resolvedUri) throws IOException {
                setUri(request, resolvedUri);
                return delegateClient.execute(request, responseHandler, context);
            }
        });
    }

    @Override
    public <T> T execute(final HttpHost target, final HttpRequest request, final ResponseHandler<? extends T> responseHandler) throws IOException {
        return loadBalancer.executeWhenResolved(toUri(target), new SmartLoadBalancer.ResolvedExecution<T>() {
            @Override
            public T executeResolved(URI resolvedUri) throws IOException {
                return delegateClient.execute(fromUri(resolvedUri), request, responseHandler);
            }
        });
    }

    @Override
    public <T> T execute(final HttpHost target, final HttpRequest request, final ResponseHandler<? extends T> responseHandler, final HttpContext context) throws IOException {
        return loadBalancer.executeWhenResolved(toUri(target), new SmartLoadBalancer.ResolvedExecution<T>() {
            @Override
            public T executeResolved(URI resolvedUri) throws IOException {
                return delegateClient.execute(fromUri(resolvedUri), request, responseHandler, context);
            }
        });
    }

    public void setRedirectStrategy(final RedirectStrategy strategy) {
        delegateClient.setRedirectStrategy(strategy);
    }

    public CredentialsProvider getCredentialsProvider() {
        return delegateClient.getCredentialsProvider();
    }

    public void addRequestInterceptor(final HttpRequestInterceptor itcp, int index) {
        delegateClient.addRequestInterceptor(itcp, index);
    }

    /**
     * Useful for examining LB configuration and statistics.
     * <p/>
     * Cast to BaseLoadBalancer and call {@link com.netflix.loadbalancer.BaseLoadBalancer#getLoadBalancerStats()} for
     * statistics summary.
     */
    public ILoadBalancer getLoadBalancer() {
        return loadBalancer.getLoadBalancer();
    }

    public void initConfig(IClientConfig nfConfig, SmartClientConfig smartConfig) {
        // "client" instance... this is just a load balancer, no client is actually created
        nfConfig.setProperty(CommonClientConfigKey.ClientClassName, SmartLoadBalancer.class.getName());

        // discovery class (queries for active node list)
        nfConfig.setProperty(CommonClientConfigKey.NIWSServerListClassName, ViPRDataServicesServerList.class.getName());

        if (smartConfig != null) {
            // VIP addresses. These are effectively the LB addresses (may just be one address).
            // If a request uses one of these "special" addresses, it is replaced with a server from the list.
            nfConfig.setProperty(CommonClientConfigKey.DeploymentContextBasedVipAddresses, smartConfig.getVipAddresses());

            // refresh interval is stored in milliseconds
            nfConfig.setProperty(CommonClientConfigKey.ServerListRefreshInterval, smartConfig.getPollInterval() * 1000);

            // set server list (polling) properties
            nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesProtocol, smartConfig.getPollProtocol());
            nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesInitialNodes, smartConfig.getInitialNodesString());
            nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesUser, smartConfig.getUser());
            nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesUserSecret, smartConfig.getSecret());
            nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesTimeout, smartConfig.getTimeout());
        }
    }

    protected void setUri(HttpUriRequest request, URI resolvedUri) {
        if (request instanceof RequestWrapper)
            ((RequestWrapper) request).setURI(resolvedUri);
        else if (request instanceof HttpRequestBase)
            ((HttpRequestBase) request).setURI(resolvedUri);
        else
            throw new SmartClientException("unsupported request type: " + request.getClass().getName());
    }

    protected URI toUri(HttpHost target) {
        try {
            return new URI(target.getSchemeName(), null, target.getHostName(), target.getPort(), null, null, null);
        } catch (URISyntaxException e) {
            throw new SmartClientException("Resolved host/port results in invalid URI", e);
        }
    }

    protected HttpHost fromUri(URI uri) {
        return new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    }

    // Concrete class to perform actual load balancing since Ribbon's HttpClient implementation does not provide it.
    // This class also incorporates a callback execution process so we can wrap HttpClient requests with load balancing
    // logic in Ribbon. This is only necessary because Ribbon tangled the LB logic with their Jersey implementation.
    public static class SmartLoadBalancer extends AbstractLoadBalancerAwareClient<ClientRequest, IResponse> {
        ThreadLocal<ResolvedExecution> threadCallback = new ThreadLocal<ResolvedExecution>();
        ThreadLocal<Object> threadResponse = new ThreadLocal<Object>();

        @Override
        public IResponse execute(ClientRequest request) throws Exception {
            Object response = threadCallback.get().executeResolved(request.getUri());
            threadResponse.set(response);

            if (response instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) response;
                if (httpResponse.getStatusLine().getStatusCode() == 503) {
                    if (httpResponse.getEntity() != null) httpResponse.getEntity().getContent().close();
                    throw new ClientException(ClientException.ErrorType.SERVER_THROTTLED);
                }
            }
            // TODO: figure out how to pull status from responseHandlers or if it's even worth it

            return new FakeResponse(request.getUri());
        }

        @SuppressWarnings("unchecked")
        public <T> T executeWhenResolved(URI originalUri, ResolvedExecution<T> callback) throws IOException {
            try {
                threadCallback.set(callback);
                executeWithLoadBalancer(new ClientRequest(originalUri));
                return (T) threadResponse.get();
            } catch (ClientException e) {
                // unwrap if possible
                Throwable t = e.getCause();
                if (t == null) throw new RuntimeException(e);
                if (t instanceof IOException) throw (IOException) t;
                else if (t instanceof RuntimeException) throw (RuntimeException) t;
                else throw new RuntimeException(t);
            } finally {
                threadCallback.remove();
                threadResponse.remove();
            }
        }

        @Override
        protected boolean isRetriableException(Throwable e) {
            if (e instanceof ClientException
                    && ((ClientException) e).getErrorType() == ClientException.ErrorType.SERVER_THROTTLED) {
                return false;
            }
            return isConnectException(e) || isSocketException(e);
        }

        @Override
        protected boolean isCircuitBreakerException(Throwable e) {
            if (e instanceof ClientException) {
                ClientException clientException = (ClientException) e;
                if (clientException.getErrorType() == ClientException.ErrorType.SERVER_THROTTLED) {
                    return true;
                }
            }
            return isConnectException(e) || isSocketException(e);
        }

        private static boolean isSocketException(Throwable e) {
            int levelCount = 0;
            while (e != null && levelCount < 10) {
                if ((e instanceof SocketException) || (e instanceof SocketTimeoutException)) {
                    return true;
                }
                e = e.getCause();
                levelCount++;
            }
            return false;
        }

        private static boolean isConnectException(Throwable e) {
            int levelCount = 0;
            while (e != null && levelCount < 10) {
                if ((e instanceof SocketException)
                        || ((e instanceof org.apache.http.conn.ConnectTimeoutException)
                        && !(e instanceof org.apache.http.conn.ConnectionPoolTimeoutException))) {
                    return true;
                }
                e = e.getCause();
                levelCount++;
            }
            return false;
        }

        protected static class FakeResponse implements IResponse {
            URI requestedUri;

            public FakeResponse(URI requestedUri) {
                this.requestedUri = requestedUri;
            }

            @Override
            public Object getPayload() throws ClientException {
                return null;
            }

            @Override
            public boolean hasPayload() {
                return false;
            }

            @Override
            public boolean isSuccess() {
                return true;
            }

            @Override
            public URI getRequestedURI() {
                return requestedUri;
            }

            @Override
            public Map<String, Collection<String>> getHeaders() {
                return null;
            }

            @Override
            public void close() {
            }
        }

        protected static interface ResolvedExecution<T> {
            public T executeResolved(URI resolvedUri) throws IOException;
        }
    }
}
