package com.emc.vipr.services.s3;

import com.amazonaws.util.StringInputStream;
import com.emc.vipr.services.s3.model.GetFileAccessResult;
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
        com.emc.vipr.services.s3.model.Object object = result.getObjects().get(0);
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
}
