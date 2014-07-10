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
package com.emc.vipr.ribbon.test;

import com.emc.vipr.ribbon.SmartClientConfigKey;
import com.emc.vipr.ribbon.ViPRDataServicesServerList;
import com.emc.vipr.services.lib.ViprConfig;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.Server;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ViPRDataServicesServerListTest {

    @Test
    public void testServerPoll() throws Exception {
        Properties viprProperties = null;
        try {
            viprProperties = ViprConfig.getProperties();
        } catch (Exception e) {
            Assume.assumeTrue("vipr.properties missing", false);
        }
        String serverString = ViprConfig.getPropertyNotEmpty(viprProperties, ViprConfig.PROP_S3_ENDPOINT);
        String user = ViprConfig.getPropertyNotEmpty(viprProperties, ViprConfig.PROP_S3_ACCESS_KEY_ID);
        String secret = ViprConfig.getPropertyNotEmpty(viprProperties, ViprConfig.PROP_S3_SECRET_KEY);

        ViPRDataServicesServerList serverList = new ViPRDataServicesServerList();
        IClientConfig nfConfig = new DefaultClientConfigImpl();
        nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesProtocol, "http");
        nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesInitialNodes, serverString.replaceAll("^https?://", ""));
        nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesUser, user);
        nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesUserSecret, secret);
        nfConfig.setProperty(SmartClientConfigKey.ViPRDataServicesTimeout, 5000);
        serverList.initWithNiwsConfig(nfConfig);

        List<Server> servers = serverList.getUpdatedListOfServers();
        Assert.assertTrue("size of server pool did not increase", servers.size() > 1);
    }

    @Test
    public void testXmlParse() throws Exception {
        String rawXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "        <ListDataNode xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "            <DataNodes> 10.247.102.239</DataNodes>\n" +
                "            <DataNodes>10.247.102.240</DataNodes>\n" +
                "            <DataNodes>10.247.102.241 </DataNodes>\n" +
                "            <VersionInfo>vipr-2.0.0.0.r2b3e482</VersionInfo>\n" +
                "        </ListDataNode>";

        HttpResponse testResponse = new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        testResponse.setEntity(new StringEntity(rawXml, ContentType.APPLICATION_XML));

        ParserTester tester = new ParserTester();
        List<String> hosts = tester.testParse(testResponse);

        Assert.assertEquals("wrong number of hosts", 3, hosts.size());
        Assert.assertEquals("wrong 1st host", "10.247.102.239", hosts.get(0));
        Assert.assertEquals("wrong 2nd host", "10.247.102.240", hosts.get(1));
        Assert.assertEquals("wrong 3rd host", "10.247.102.241", hosts.get(2));
    }

    private class ParserTester extends ViPRDataServicesServerList {
        public List<String> testParse(HttpResponse response) throws IOException, JAXBException {
            return this.parseResponse(response);
        }
    }
}
