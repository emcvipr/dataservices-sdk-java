package com.emc.vipr.transform.compression;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.encryption.KeyUtils;
import com.emc.vipr.transform.util.CountingInputStream;

public class DeflateInputFilter extends InputStream implements
        CompressionStream {
    private CountingInputStream uncompressedCounter;
    private CountingInputStream compressedCounter;
    private DigestInputStream digester;
    private boolean closed;
    private byte[] uncompressedDigest;

    public DeflateInputFilter(InputStream in, int level) throws IOException {
        Deflater def = new Deflater(level);
        uncompressedCounter = new CountingInputStream(in);
        try {
            digester = new DigestInputStream(uncompressedCounter, MessageDigest.getInstance("SHA1"));
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("Unable to initialize digest", e);
        }
        DeflaterInputStream deflateFilter = new DeflaterInputStream(digester, def);
        compressedCounter = new CountingInputStream(deflateFilter);
    }
    
    @Override
    public int read() throws IOException {
        return compressedCounter.read();
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return compressedCounter.read(b, off, len);
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return compressedCounter.read(b);
    }
    
    @Override
    public void close() throws IOException {
        if(closed) return;
        closed = true;
        compressedCounter.close();
        // Store digest (can only call this once).
        uncompressedDigest = digester.getMessageDigest().digest();
    }
    
    @Override
    public int available() throws IOException {
        return compressedCounter.available();
    }

    @Override
    public Map<String, String> getStreamMetadata() {
        if(!closed) {
            throw new IllegalStateException("Stream must be closed before getting metadata");
        }
        
        Map<String,String> metadata = new HashMap<String, String>();
        
        long compSize = compressedCounter.getByteCount();
        long uncompSize = uncompressedCounter.getByteCount();
        String compRatioString = String.format("%.1f%%", 100.0 - (compSize*100.0/uncompSize));
        
        metadata.put(TransformConstants.META_COMPRESSION_UNCOMP_SIZE, ""+uncompSize);
        metadata.put(TransformConstants.META_COMPRESSION_COMP_SIZE, ""+compSize);
        metadata.put(TransformConstants.META_COMPRESSION_COMP_RATIO, ""+compRatioString);
        metadata.put(TransformConstants.META_COMPRESSION_UNCOMP_SHA1, KeyUtils.toHexPadded(uncompressedDigest));
        
        return metadata;
    }

}
