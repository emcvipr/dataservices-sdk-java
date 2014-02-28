package com.emc.vipr.transform.encryption;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

import com.emc.vipr.transform.util.CountingInputStream;

public class EncryptionInputFilter extends InputStream {
    boolean closed = false;
    byte[] digest = null;
    private DigestInputStream digestStream;
    private CountingInputStream counterStream;
    private CipherInputStream cipherStream;

    public EncryptionInputFilter(InputStream in, Cipher cipher, MessageDigest digest) {
        // Construct the filter chain:
        // user stream->CountingInputStream->
        // DigestInputStream(optional)->CipherInputStream
        counterStream = new CountingInputStream(in);
        if(digest != null) {
            digestStream = new DigestInputStream(counterStream, digest);
            cipherStream = new CipherInputStream(digestStream, cipher);
        } else {
            cipherStream = new CipherInputStream(counterStream, cipher);
        }
    }

    @Override
    public int read() throws IOException {
        return cipherStream.read();
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return cipherStream.read(b);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return cipherStream.read(b, off, len);
    }
    
    @Override
    public void close() throws IOException {
        if(closed) return;
        closed = true;
        
        cipherStream.close();
        
        if(digestStream != null) {
            // Get the digest.  We can only do this once due to the way 
            // MessageDigest works.
            digest = digestStream.getMessageDigest().digest();
        }
    }
    
    @Override
    public int available() throws IOException {
        return cipherStream.available();
    }
    
    @Override
    public long skip(long n) throws IOException {
        return cipherStream.skip(n);
    }

    public byte[] getDigest() {
        if(!closed) {
            throw new IllegalStateException("Cannot get digest until stream is closed");
        }
        return digest;
    }

    public long getByteCount() {
        return counterStream.getByteCount();
    }

}
