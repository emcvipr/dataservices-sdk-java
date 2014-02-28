package com.emc.vipr.transform.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import SevenZip.Compression.LZMA.Encoder;

import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.compression.CompressionTransformFactory.LzmaProfile;
import com.emc.vipr.transform.encryption.KeyUtils;
import com.emc.vipr.transform.util.CountingInputStream;

/**
 * Compression filter used in "pull" mode to compress data for the
 * @link {@link CompressionOutputTransform}.
 */
public class LZMACompressionFilter extends InputStream implements CompressionStream, Runnable {
    private static Logger log = LoggerFactory.getLogger(LZMACompressionFilter.class);
    
    private boolean closed = false;
    private Thread compressionThread;
    private PipedInputStream inputPipe;
    private PipedOutputStream outputPipe;
    private Encoder lzma;
    private CountingInputStream uncompressedSize;
    private CountingInputStream compressedSize;
    private DigestInputStream uncompressedDigest;
    private byte[] digest;
    private Exception compressionFailure;

    public LZMACompressionFilter(InputStream in, int compressionLevel) throws IOException {
        this(in, CompressionTransformFactory.LZMA_COMPRESSION_PROFILE[compressionLevel]);
    }
    
    public LZMACompressionFilter(InputStream in, LzmaProfile compressionProfile) throws IOException {
        closed = false;
        digest = new byte[0];
        
        // The LZMA Encoder reads from an input stream and writes to an output stream and 
        // thus does not make a good filter.  We need to create a pipe and and use an 
        // auxiliary thread to compress the data.
        //
        // Filter chain:
        // user stream -> CountingInputStream(uncompressedSize) -> DigestInputStream ->
        // Encoder -> PipedOutputStream -> PipedInputStream -> 
        // CountingInputStream(compressedSize)
        uncompressedSize = new CountingInputStream(in);
        try {
            uncompressedDigest = new DigestInputStream(uncompressedSize, 
                    MessageDigest.getInstance("SHA1"));
        } catch (NoSuchAlgorithmException e) {
           throw new IOException("Could not create LZMACompessionFilter", e);
        }
        inputPipe = new PipedInputStream();
        outputPipe = new PipedOutputStream(inputPipe);
        
        compressedSize = new CountingInputStream(inputPipe);
        lzma = new Encoder();
        lzma.SetDictionarySize(compressionProfile.dictionarySize);
        lzma.SetNumFastBytes(compressionProfile.fastBytes);
        lzma.SetMatchFinder(compressionProfile.matchFinder);
        lzma.SetLcLpPb(compressionProfile.lc, compressionProfile.lp, compressionProfile.pb);
        lzma.SetEndMarkerMode(true);
        
        // Write the compression settings to the stream (this is read during
        // decompression to configure the decoder)
        lzma.WriteCoderProperties(outputPipe);
        
        compressionThread = new Thread(LZMAOutputStream.LZ_COMP_TG, this);
        
        compressionThread.start();
    }

    @Override
    public int read() throws IOException {
        readCheck();
        return compressedSize.read();
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        readCheck();
        return compressedSize.read(b);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        readCheck();
        return compressedSize.read(b, off, len);
    }
    
    @Override
    public int available() throws IOException {
        readCheck();
        return compressedSize.available();
    }
    
    @Override
    public long skip(long n) throws IOException {
        readCheck();
        return compressedSize.skip(n);
    }
    
    @Override
    public void close() throws IOException {
        if(closed) return;
        closed = true;
        
        compressedSize.close();
        
        // Wait for encoder to finish
        try {
            compressionThread.join();
        } catch (InterruptedException e) {
            throw new IOException("Error waiting for compression thread to exit", e);
        }

        digest = uncompressedDigest.getMessageDigest().digest();
        
        // Free the encoder
        lzma = null;
    }

    @Override
    public void run() {
        // Start compressing data
        try {
            lzma.Code(uncompressedDigest, outputPipe, -1, -1, null);
        } catch(Exception e) {
            compressionFailure(e);
        }
        
        // Compression done.  Close output side of pipe before thread dies.
        try {
            outputPipe.close();
        } catch (IOException e) {
            compressionFailure(e);
        }

    }
    
    private synchronized void compressionFailure(Exception e) {
        compressionFailure = e;
        log.error("Error compressing data", e);
    }
    
    private synchronized void readCheck() throws IOException {
        if(compressionFailure != null) {
            throw new IOException("Error during stream compression", compressionFailure);
        }
        if(closed) {
            throw new IOException("Stream closed");
        }
    }

    @Override
    public Map<String, String> getStreamMetadata() {
        if(!closed) {
            throw new IllegalStateException("Stream must be closed before getting metadata");
        }
        
        Map<String,String> metadata = new HashMap<String, String>();
        
        long compSize = compressedSize.getByteCount();
        long uncompSize = uncompressedSize.getByteCount();
        String compRatioString = String.format("%.1f%%", 100.0 - (compSize*100.0/uncompSize));
        
        metadata.put(TransformConstants.META_COMPRESSION_UNCOMP_SIZE, ""+uncompSize);
        metadata.put(TransformConstants.META_COMPRESSION_COMP_SIZE, ""+compSize);
        metadata.put(TransformConstants.META_COMPRESSION_COMP_RATIO, ""+compRatioString);
        metadata.put(TransformConstants.META_COMPRESSION_UNCOMP_SHA1, 
                KeyUtils.toHexPadded(digest));
        
        return metadata;
    }

}
