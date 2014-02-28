package com.emc.vipr.transform;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public abstract class OutputTransform {
    public enum StreamMode { PUSH, PULL };
    
    protected OutputStream pushStream;
    protected InputStream pullStream;
    protected Map<String, String> metadataToEncode;
    private String transformConfig;
    private StreamMode mode;

    public OutputTransform(OutputStream streamToEncode, Map<String, String> metadataToEncode, String transformConfig) {
        this.pushStream = streamToEncode;
        this.metadataToEncode = metadataToEncode;
        this.transformConfig = transformConfig;
        mode = StreamMode.PUSH;
    }
    
    public OutputTransform(InputStream streamtoEncode, Map<String, String> metadataToEncode, String transformConfig) {
        this.pullStream = streamtoEncode;
        this.metadataToEncode = metadataToEncode;
        this.transformConfig = transformConfig;
        mode = StreamMode.PULL;
    }
    
    public StreamMode getStreamMode() { 
        return mode;
    }
    
    /**
     * Wraps the output stream with an encoder that will apply this transformation to
     * the stream.
     * @return a new output stream object that encodes the source stream.
     */
    public OutputStream getEncodedOutputStream() {
        if(mode != StreamMode.PUSH) {
            throw new IllegalStateException("Cannot get output stream in pull mode");
        }
        return pushStream;
    }
    
    public InputStream getEncodedInputStream() {
        if(mode != StreamMode.PULL) {
            throw new IllegalStateException("Cannot get output stream in pull mode");
        }
        return pullStream;
    }

    /**
     * Encodes the object's metadata. Usually, this is called
     * after the output stream has been closed to get the updated object
     * metadata but could also somehow transform existing metadata like encrypting it.
     * @return the "encoded" metadata
     */
    public abstract Map<String, String> getEncodedMetadata();

    /**
     * Gets this transformation's mode string, e.g. "COMP:LZMA/3"
     * @return the mode string.
     */
    public String getTransformConfig() {
        return transformConfig;
    }

}
