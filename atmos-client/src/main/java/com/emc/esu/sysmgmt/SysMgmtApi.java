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
package com.emc.esu.sysmgmt;

import com.emc.esu.sysmgmt.pox.GetUidRequest;
import com.emc.esu.sysmgmt.pox.GetUidResponse;
import com.emc.esu.sysmgmt.pox.ListRmgRequestPox;
import com.emc.esu.sysmgmt.pox.ListRmgResponsePox;
import com.emc.util.HttpUtil;
import org.apache.log4j.Logger;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main Façade for the Atmos system management API.
 * 
 * @author cwikj
 */
public class SysMgmtApi {
	private static final Logger l4j = Logger.getLogger(SysMgmtApi.class);
	
	private String proto;
	private String host;
	private int port;
	private String username;
	private String password;
	private String poxCookie;

	/**
	 * Default constructor
	 */
	public SysMgmtApi() {
		port = 443;
		proto = "https";
	}

	/**
	 * Constructs a new SysMgmtApi connection
	 * 
	 * @param proto
	 *            The protocol to use, generally "https"
	 * @param host
	 *            The host to connect to. Can be any Atmos host.
	 * @param port
	 *            The port to connect to. Generally 443 for https.
	 * @param username
	 *            Your SysAdmin user.
	 * @param password
	 *            Your SysAdmin password.
	 */
	public SysMgmtApi(String proto, String host, int port, String username,
			String password) {
		this.proto = proto;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
	}

	/**
	 * Lists all of the RMGs in your cloud.
	 * 
	 * @return a ListRmgResponse
	 */
	public ListRmgResponse listRmgs() {
		ListRmgRequest req = new ListRmgRequest(this);
		return req.call();
	}
	
	public ListRmgResponsePox listRmgsPox() throws Exception {
		if(poxCookie == null) {
			throw new RuntimeException("Not logged in to POX");
		}
		ListRmgRequestPox req = new ListRmgRequestPox(this);
		return req.call();
	}

	/**
	 * Lists all of the hosts in the given RMG.
	 * 
	 * @param rmgName
	 *            the name of the RMG to list
	 * @return a ListHostsResponse
	 */
	public ListHostsResponse listHosts(String rmgName) {
		ListHostsRequest req = new ListHostsRequest(this, rmgName);
		return req.call();
	}

	/**
	 * Returns the current connection protocol
	 * 
	 * @return the proto
	 */
	public String getProto() {
		return proto;
	}

	/**
	 * Sets the current connection protocol.
	 * 
	 * @param proto
	 *            the proto to set
	 */
	public void setProto(String proto) {
		this.proto = proto;
	}

	/**
	 * Gets the current hostname
	 * 
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Sets the hostname to connect to
	 * 
	 * @param host
	 *            the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Gets the current connection port
	 * 
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port
	 *            the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Gets the current SysAdmin username
	 * 
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Sets the SysAdmin username to connect with
	 * 
	 * @param username
	 *            the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Sets the SysAdmin password to use.
	 * 
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}
	
	/**
	 * Returns the current SysAdmin password.  Note that this method is only
	 * package-level access for security reasons.
	 * 
	 * @return the password
	 */
	String getPassword() {
		return this.password;
	}

	/**
	 * Many Atmos systems have self-signed certificates for their system admin
	 * consoles. If so, you will need to call this method to install a special
	 * certificate validator and hostname verifier so an Exception is not thrown
	 * when connecting to the server. <em>NOTE</em> currently, this installs a
	 * default verifier for ALL connections, even non-Atmos systems.
	 * 
	 * @throws NoSuchAlgorithmException
	 * @throws KeyManagementException
	 */
	public static void disableCertificateValidation()
			throws NoSuchAlgorithmException, KeyManagementException {
		// TODO make this only apply to the current hostname.

		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(X509Certificate[] certs,
					String authType) {
			}

			public void checkServerTrusted(X509Certificate[] certs,
					String authType) {
			}
		} };

		// Install the all-trusting trust manager
		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

		// Create all-trusting host name verifier
		HostnameVerifier allHostsValid = new HostnameVerifier() {
			public boolean verify(String hostname, SSLSession session) {
				return true;
			}
		};

		// Install the all-trusting host verifier
		HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
	}

	/**
	 * @return the poxCookie
	 */
	public String getPoxCookie() {
		return poxCookie;
	}
	
	public void poxLogin(String tenantName, String tenantAdmin, String tenantAdminPassword) throws IOException, URISyntaxException {
        URI uri = new URI( proto, null, host, 
        		port, "/user/verify", null, null );
        l4j.debug("URI: " + uri);
        URL u = new URL(uri.toASCIIString());
        l4j.debug( "URL: " + u );

        HttpURLConnection con = (HttpURLConnection) u.openConnection();
        
        con.addRequestProperty("Accept", "application/xml");
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.connect();
        OutputStream out = con.getOutputStream();
        String urlTenantName = HttpUtil.encodeUtf8(tenantName);
        String urluser = HttpUtil.encodeUtf8(tenantAdmin);
        String urlpass = HttpUtil.encodeUtf8(tenantAdminPassword);
        String requestBody = "tenant_name=" + urlTenantName + "&username=" + 
        		urluser + "&password=" + urlpass;
        out.write(requestBody.getBytes("US-ASCII"));
        out.close();
        
		int code = con.getResponseCode();
		if(code != 200) {
			throw new RuntimeException("Login failed with HTTP code " + code);
		}
		
		// Extract cookie
		String cookies = con.getHeaderField("Set-Cookie");
		
        Pattern cookiePattern = Pattern.compile("_gui_session_id=([^;]+);.*");
		Matcher m = cookiePattern.matcher(cookies);
		if(!m.find()) {
			throw new RuntimeException("Could not parse session cookie from " + cookies);
		}
		poxCookie = m.group(1);
		
	}
	
	public void poxLogin() throws IOException, URISyntaxException {
        URI uri = new URI( proto, null, host, 
        		port, "/mgmt_login/verify", null, null );
        l4j.debug("URI: " + uri);
        URL u = new URL(uri.toASCIIString());
        l4j.debug( "URL: " + u );

        HttpURLConnection con = (HttpURLConnection) u.openConnection();
        
        con.addRequestProperty("Accept", "application/xml");
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.connect();
        OutputStream out = con.getOutputStream();
        String urluser = HttpUtil.encodeUtf8(username);
        String urlpass = HttpUtil.encodeUtf8(password);
        String requestBody = "auth_type=local&auth_addr=&username=" + 
        		urluser + "&password=" + urlpass;
        out.write(requestBody.getBytes("US-ASCII"));
        out.close();
        
		int code = con.getResponseCode();
		if(code != 200) {
			throw new RuntimeException("Login failed with HTTP code " + code);
		}
		
		// Extract cookie
		String cookies = con.getHeaderField("Set-Cookie");
		
		Pattern cookiePattern = Pattern.compile("_gui_session_id=([^;]+);.*");
		Matcher m = cookiePattern.matcher(cookies);
		if(!m.find()) {
			throw new RuntimeException("Could not parse session cookie from " + cookies);
		}
		poxCookie = m.group(1);
	}

	public GetUidResponse getUidPox(String subTenantName, String uid) throws Exception {
		GetUidRequest req = new GetUidRequest(this);
		req.setUid(uid);
		req.setSubTenantName(subTenantName);
		
		return req.call();
	}

}
