/**
 * 
 */
package com.emc.vipr.transform.compression;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.encryption.KeyUtils;
import com.emc.vipr.transform.util.CountingOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * @author cwikj
 *
 */
public class DeflateOutputStream extends OutputStream implements CompressionStream  {
    private CountingOutputStream uncompressedCounter;
    private CountingOutputStream compressedCounter;
    private DigestOutputStream digester;
    private boolean closed;
    private byte[] uncompressedDigest;

    /**
     * @throws IOException
     */
    public DeflateOutputStream(OutputStream streamToCompress, int level) throws IOException {
        Deflater def = new Deflater(level);
        compressedCounter = new CountingOutputStream(streamToCompress);
        DeflaterOutputStream dos = new DeflaterOutputStream(compressedCounter, def);
        uncompressedCounter = new CountingOutputStream(dos);
        try {
            digester = new DigestOutputStream(uncompressedCounter, MessageDigest.getInstance("SHA1"));
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("Unable to initialize digest", e);
        }
        closed = false;
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

    @Override
    public void write(int b) throws IOException {
        digester.write(b);
        
    }

    @Override
    public void close() throws IOException {
        if(closed) { return; }
        closed = true;
        digester.close();
        uncompressedDigest = digester.getMessageDigest().digest();
    }

    @Override
    public void flush() throws IOException {
        digester.flush();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        digester.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        digester.write(b);
    }
    
    

}
