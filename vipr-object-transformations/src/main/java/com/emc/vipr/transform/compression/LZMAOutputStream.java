/**
 * 
 */
package com.emc.vipr.transform.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.compression.CompressionTransformFactory.LzmaProfile;
import com.emc.vipr.transform.encryption.KeyUtils;
import com.emc.vipr.transform.util.CountingOutputStream;

import SevenZip.Compression.LZMA.Encoder;

/**
 * @author cwikj
 *
 */
public class LZMAOutputStream extends OutputStream implements CompressionStream, Runnable {
    private static final Logger log = LoggerFactory.getLogger(LZMAOutputStream.class);
    
    private CountingOutputStream compressedOutput;
    private Thread compressionThread;
    private InputStream inputPipe;
    private CountingOutputStream uncompressedSize;
    private DigestOutputStream outputPipe;
    private boolean closed;
    private Encoder lzma;
    private Exception compressionFailure;
    private byte[] uncompressedDigest;
    
    
    static ThreadGroup LZ_COMP_TG = new ThreadGroup("LZMACompress");
    
    public LZMAOutputStream(OutputStream out, LzmaProfile compressionProfile) throws IOException {
        compressedOutput = new CountingOutputStream(out);
        closed = false;
        uncompressedDigest = new byte[0];
        
        // The LZMA Encoder requires an input stream and thus does not make a good
        // filter.  We need to create a pipe and and use an auxiliary thread to compress
        // the data.
        inputPipe = new PipedInputStream();
        uncompressedSize = new CountingOutputStream(new PipedOutputStream((PipedInputStream) inputPipe));
        try {
            outputPipe = new DigestOutputStream(uncompressedSize, MessageDigest.getInstance("SHA1"));
        } catch (NoSuchAlgorithmException e) {
           throw new IOException("Could not create LZMAOutputStream", e);
        }
        lzma = new Encoder();
        lzma.SetDictionarySize(compressionProfile.dictionarySize);
        lzma.SetNumFastBytes(compressionProfile.fastBytes);
        lzma.SetMatchFinder(compressionProfile.matchFinder);
        lzma.SetLcLpPb(compressionProfile.lc, compressionProfile.lp, compressionProfile.pb);
        lzma.SetEndMarkerMode(true);
        
        lzma.WriteCoderProperties(compressedOutput);
        
        compressionThread = new Thread(LZ_COMP_TG, this);
        
        compressionThread.start();
    }

    public LZMAOutputStream(OutputStream out, int compressionLevel) throws IOException {
        this(out, CompressionTransformFactory.LZMA_COMPRESSION_PROFILE[compressionLevel]);
    }

    /*
     * @see com.emc.vipr.transform.compression.CompressionOutputStream#getStreamMetadata()
     */
    @Override
    public Map<String, String> getStreamMetadata() {
        if(!closed) {
            throw new IllegalStateException("Stream must be closed before getting metadata");
        }
        
        Map<String,String> metadata = new HashMap<String, String>();
        
        long compSize = compressedOutput.getByteCount();
        long uncompSize = uncompressedSize.getByteCount();
        String compRatioString = String.format("%.1f%%", 100.0 - (compSize*100.0/uncompSize));
        
        metadata.put(TransformConstants.META_COMPRESSION_UNCOMP_SIZE, ""+uncompSize);
        metadata.put(TransformConstants.META_COMPRESSION_COMP_SIZE, ""+compSize);
        metadata.put(TransformConstants.META_COMPRESSION_COMP_RATIO, ""+compRatioString);
        metadata.put(TransformConstants.META_COMPRESSION_UNCOMP_SHA1, KeyUtils.toHexPadded(uncompressedDigest));
        
        return metadata;
    }

    @Override
    public void write(int b) throws IOException {
        outputPipe.write(b);
    }

    @Override
    public void close() throws IOException {
        if(closed) { return; }
        outputPipe.flush();
        outputPipe.close();

        closed = true;
        
        // Wait for encoder to finish
        try {
            compressionThread.join();
        } catch (InterruptedException e) {
            throw new IOException("Error waiting for compression thread to exit", e);
        }
        
        compressedOutput.close();
        uncompressedDigest = outputPipe.getMessageDigest().digest();
        // Free the encoder
        lzma = null;
        
    }

    @Override
    public void flush() throws IOException {
        writeCheck();
        outputPipe.flush();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        writeCheck();
        outputPipe.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        writeCheck();
        write(b, 0, b.length);
    }

    @Override
    public void run() {
        // Start compressing data
        try {
            lzma.Code(inputPipe, compressedOutput, -1, -1, null);
        } catch(Exception e) {
            compressionFailure(e);
        }

    }
    
    private synchronized void compressionFailure(Exception e) {
        compressionFailure = e;
        log.error("Error compressing data", e);
    }
    
    private synchronized void writeCheck() throws IOException {
        if(compressionFailure != null) {
            throw new IOException("Error during stream compression", compressionFailure);
        }
        if(closed) {
            throw new IOException("Stream closed");
        }
    }
    
    

}
