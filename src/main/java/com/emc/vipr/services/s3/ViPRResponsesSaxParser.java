package com.emc.vipr.services.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser;
import com.emc.vipr.services.s3.model.GetFileAccessResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ViPRResponsesSaxParser extends XmlResponsesSaxParser {
    private static final Log log = LogFactory.getLog(ViPRResponsesSaxParser.class);

    /**
     * Parses a fileaccess response XML document for a bucket from an input
     * stream.
     *
     * @param inputStream XML data input stream.
     * @return the XML handler object populated with data parsed from the XML
     *         stream.
     * @throws com.amazonaws.AmazonClientException
     *
     */
    public FileAccessResultHandler parseFileAccessResult(InputStream inputStream)
            throws AmazonClientException {
        FileAccessResultHandler handler = new FileAccessResultHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    /*
        <?xml version="1.0" encoding="UTF-8"?>
        <fileaccess_response>
            <mountPoints>no_idea_what_goes_here</mountPoints>
            <mountPoints>no_idea_what_goes_here</mountPoints>
            <objects>
               <name>foo</name>
               <deviceExport>cifs://foo.foo/foo</deviceExport>
               <relativePath>foo</relativePath>
               <owner>some_guy</owner>
            </objects>
            <objects>
               <name>blah</name>
               <deviceExport>nfs://blahblah:/blah</deviceExport>
               <relativePath>blah</relativePath>
               <owner>some_girl</owner>
            </objects>
        </fileaccess_response>
     */
    public class FileAccessResultHandler extends DefaultHandler {
        private GetFileAccessResult result = new GetFileAccessResult();
        private StringBuilder text;
        private List<String> mountPoints = new ArrayList<String>();
        private List<com.emc.vipr.services.s3.model.Object> objects = new ArrayList<com.emc.vipr.services.s3.model.Object>();
        private com.emc.vipr.services.s3.model.Object object, lastObject;

        public GetFileAccessResult getResult() {
            return result;
        }

        @Override
        public void startDocument() {
            text = new StringBuilder();
        }

        @Override
        public void startElement(String uri, String name, String qName, Attributes attrs) {
            if (name.equals("fileaccess_response")) {
                // expected, but no action
            } else if (name.equals("mountPoints")) {
                text.setLength(0);
            } else if (name.equals("hasMore")) {
                text.setLength(0);
            } else if (name.equals("name")) {
                text.setLength(0);
            } else if (name.equals("deviceExport")) {
                text.setLength(0);
            } else if (name.equals("relativePath")) {
                text.setLength(0);
            } else if (name.equals("owner")) {
                text.setLength(0);
            } else if (name.equals("objects")) {
                object = new com.emc.vipr.services.s3.model.Object();
            } else {
                log.warn("Ignoring unexpected tag <" + name + ">");
            }
        }

        @Override
        public void endElement(String uri, String name, String qName) throws SAXException {
            if (name.equals("mountPoints")) {
                mountPoints.add(text.toString());
            } else if (name.equals("hasMore")) {
                result.setTruncated(Boolean.parseBoolean(text.toString()));
            } else if (name.equals("name") && object != null) {
                object.setName(text.toString());
            } else if (name.equals("deviceExport") && object != null) {
                object.setDeviceExport(text.toString());
            } else if (name.equals("relativePath") && object != null) {
                object.setRelativePath(text.toString());
            } else if (name.equals("owner") && object != null) {
                object.setOwner(text.toString());
            } else if (name.equals("objects")) {
                objects.add(object);
                lastObject = object;
                object = null;
            }
            text.setLength(0);
        }

        @Override
        public void characters(char ch[], int start, int length) {
            this.text.append(ch, start, length);
        }

        @Override
        public void endDocument() throws SAXException {
            result.setMountPoints(mountPoints);
            result.setObjects(objects);
            if (result.isTruncated()) result.setLastKey(lastObject.getName());
        }
    }
}
