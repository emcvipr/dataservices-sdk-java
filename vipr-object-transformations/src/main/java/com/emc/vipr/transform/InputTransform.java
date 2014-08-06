/**
 * 
 */
package com.emc.vipr.transform;

import java.io.InputStream;
import java.util.Map;

/**
 * @author cwikj
 *
 */
public abstract class InputTransform {

    protected InputStream streamToDecode;
    protected Map<String, String> metadataToDecode;

    /**
     * 
     */
    public InputTransform(InputStream streamToDecode, Map<String,String> metadataToDecode) {
        this.streamToDecode = streamToDecode;
        this.metadataToDecode = metadataToDecode;
    }

    /**
     * Wraps an input stream with another stream that will decode the inbound object
     * data stream.
     * @return the input stream wrapped with a decoder.
     */
    public abstract InputStream getDecodedInputStream();

    /**
     * Decodes the object's metadata.  Usually, this will simply return the object's
     * metadata.  However, in some circumstances this method could apply some
     * transformation to the metadata like decrypting it.
     * @return the decoded metadata.
     */
    public Map<String, String> getDecodedMetadata() {
        return metadataToDecode;
    }
}
