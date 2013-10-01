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
package com.emc.esu.test;

import com.emc.atmos.util.AtmosClientFactory;
import com.emc.esu.sysmgmt.ListHostsResponse;
import com.emc.esu.sysmgmt.ListRmgResponse;
import com.emc.esu.sysmgmt.SysMgmtApi;
import com.emc.esu.sysmgmt.SysMgmtResponse;
import com.emc.esu.sysmgmt.pox.GetUidResponse;
import com.emc.esu.sysmgmt.pox.ListRmgResponsePox;
import com.emc.util.PropertiesUtil;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Assume;
import org.junit.Test;

/**
 * @author cwikj
 *
 */
public class EsuSysMgmtApiTest {
	private static final Logger l4j = Logger.getLogger(EsuSysMgmtApiTest.class);
	
	private String proto;
	private String host;
	private int port;
	private String username;
	private String password;
	private SysMgmtApi api;

	public EsuSysMgmtApiTest() {
    	proto = PropertiesUtil.getProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.sysmgmt.proto");
    	Assume.assumeTrue("atmos.sysmgmt.proto is null", proto != null);
//    	if( proto == null ) {
//    		throw new RuntimeException( "atmos.sysmgmt.proto is null.  Set in atmos.properties or on command line with -Datmos.sysmgmt.proto" );
//    	}
    	host = PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.sysmgmt.host");
    	port = Integer.parseInt( PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.sysmgmt.port") );
    	
    	username = PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.sysmgmt.username");
    	password = PropertiesUtil.getRequiredProperty(AtmosClientFactory.ATMOS_PROPERTIES_FILE, "atmos.sysmgmt.password");
    	
    	api = new SysMgmtApi(proto, host, port, username, password);
    	
    	try {
			SysMgmtApi.disableCertificateValidation();
		} catch (Exception e) {
			throw new RuntimeException("Could not disable certificate validation");
		}
	}
	
	@Test
	public void testListRmgs() {
		ListRmgResponse resp = api.listRmgs();
		
		checkResponse(resp);
		
		Assert.assertNotNull("RMG list null", resp.getRmgs());
		Assert.assertTrue("Expected at least 1 RMG", resp.getRmgs().size()>0);
		for(ListRmgResponse.Rmg r : resp.getRmgs()) {
			l4j.debug("RMG:" + r);
			Assert.assertNotNull("RMG name null", r.getName());
			Assert.assertNotNull("RMG localtime null", r.getLocalTime());
			Assert.assertNotNull("RMG tostring null", r.toString());
		}
	}
	
	@Test
	public void testListHosts() {
		ListRmgResponse resp = api.listRmgs();
		
		checkResponse(resp);
		
		Assert.assertNotNull("RMG list null", resp.getRmgs());
		Assert.assertTrue("Expected at least 1 RMG", resp.getRmgs().size()>0);
		for(ListRmgResponse.Rmg r : resp.getRmgs()) {
			ListHostsResponse hresp = api.listHosts(r.getName());
			checkResponse(hresp);
			Assert.assertTrue("Expected at least two hosts in an RMG", hresp.getHosts().size()>1);
			for(ListHostsResponse.Host h : hresp.getHosts()) {
				l4j.debug("Host: " + h);
				Assert.assertNotNull("Host name null", h.getName());
				Assert.assertNotNull("location null", h.getLocation());
			}
		}
	}
	
	@Test
	public void testPoxLogin() throws Exception {
		api.poxLogin();
	}
	
	@Test
	public void testListRmgsPox() throws Exception {
		api.poxLogin();
		ListRmgResponsePox resp = api.listRmgsPox();
		
		Assert.assertNotNull("RMG list null", resp.getRmgs());
		Assert.assertTrue("Expected at least 1 RMG", resp.getRmgs().size()>0);
		
	}
	
	@Test
	public void testGetUidPox() throws Exception {
		api.poxLogin("Tenant1", "TenantAdmin", "password");
		GetUidResponse resp = api.getUidPox("zimbra", "zimbra");
		
		Assert.assertNotNull("UID response null", resp);
	}

	private void checkResponse(SysMgmtResponse resp) {
		Assert.assertNotNull("Response was null", resp);
		Assert.assertNotNull("Server date was null", resp.getServerDate());
		Assert.assertNotNull("Sysmgmt version was null", resp.getAtmosSysMgmtVersion());
	}
}
