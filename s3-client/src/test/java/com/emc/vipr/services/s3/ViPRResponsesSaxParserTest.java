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
package com.emc.vipr.services.s3;

import com.amazonaws.util.StringInputStream;
import com.emc.vipr.services.s3.model.FileAccessObject;
import com.emc.vipr.services.s3.model.GetFileAccessResult;
import com.emc.vipr.services.s3.model.ListDataNodesResult;
import org.junit.Assert;
import org.junit.Test;

/*
 * Tests parsing ViPR-specific S3 XML responses.
 */
public class ViPRResponsesSaxParserTest {
    @Test
    public void testFileAccessResult() throws Exception {
        String rawXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "        <fileaccess_response>\n" +
                "            <mountPoints>no_idea_what_goes_here</mountPoints>\n" +
                "            <mountPoints>no_idea_what_goes_here</mountPoints>\n" +
                "            <objects>\n" +
                "               <name>foo</name>\n" +
                "               <deviceExport>cifs://foo.foo/foo</deviceExport>\n" +
                "               <relativePath>foo</relativePath>\n" +
                "               <owner>some_guy</owner>\n" +
                "            </objects>\n" +
                "            <objects>\n" +
                "               <name>blah</name>\n" +
                "               <deviceExport>nfs://blahblah:/blah</deviceExport>\n" +
                "               <relativePath>blah</relativePath>\n" +
                "               <owner>some_girl</owner>\n" +
                "            </objects>\n" +
                "        </fileaccess_response>";

        GetFileAccessResult result = new ViPRResponsesSaxParser().parseFileAccessResult(new StringInputStream(rawXml)).getResult();

        Assert.assertNotNull("result is null", result);
        Assert.assertEquals("wrong mount point number", 2, result.getMountPoints().size());
        Assert.assertEquals("wrong object number", 2, result.getObjects().size());
        Assert.assertEquals("wrong mount point value", "no_idea_what_goes_here", result.getMountPoints().get(0));
        Assert.assertEquals("wrong mount point value", "no_idea_what_goes_here", result.getMountPoints().get(1));
        FileAccessObject object = result.getObjects().get(0);
        Assert.assertEquals("wrong object name", "foo", object.getName());
        Assert.assertEquals("wrong object export", "cifs://foo.foo/foo", object.getDeviceExport());
        Assert.assertEquals("wrong object relative path", "foo", object.getRelativePath());
        Assert.assertEquals("wrong object owner", "some_guy", object.getOwner());
        object = result.getObjects().get(1);
        Assert.assertEquals("wrong object name", "blah", object.getName());
        Assert.assertEquals("wrong object export", "nfs://blahblah:/blah", object.getDeviceExport());
        Assert.assertEquals("wrong object relative path", "blah", object.getRelativePath());
        Assert.assertEquals("wrong object owner", "some_girl", object.getOwner());
    }

    @Test
    public void testDataNodesResult() throws Exception {
        String rawXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "        <ListDataNode xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "            <DataNodes> 10.247.102.239</DataNodes>\n" +
                "            <DataNodes>10.247.102.240</DataNodes>\n" +
                "            <DataNodes>10.247.102.241 </DataNodes>\n" +
                "            <VersionInfo>vipr-2.0.0.0.r2b3e482</VersionInfo>\n" +
                "        </ListDataNode>";

        ListDataNodesResult result = new ViPRResponsesSaxParser().parseListDataNodeResult(new StringInputStream(rawXml)).getResult();

        Assert.assertNotNull("result is null", result);
        Assert.assertEquals("wrong version", "vipr-2.0.0.0.r2b3e482", result.getVersion());
        Assert.assertEquals("wrong number of hosts", 3, result.getHosts().size());
        Assert.assertEquals("wrong 1st host", "10.247.102.239", result.getHosts().get(0));
        Assert.assertEquals("wrong 2nd host", "10.247.102.240", result.getHosts().get(1));
        Assert.assertEquals("wrong 3rd host", "10.247.102.241", result.getHosts().get(2));
    }
}
