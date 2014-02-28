package com.emc.vipr.transform.encryption;

import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

import com.emc.vipr.transform.util.CountingOutputStream;

public class EncryptionOutputStream extends OutputStream {

    boolean closed = false;
    byte[] digest = null;
    private DigestOutputStream digestStream;
    private CountingOutputStream counterStream;

    public EncryptionOutputStream(OutputStream out, Cipher cipher, MessageDigest digest) {        
        // Create the stream chain:
        // CountingOutputStream->DigestOutputStream(opt)->CipherOutputStream->
        // user output stream.
        CipherOutputStream cipherStream = new CipherOutputStream(out, cipher);
        if(digest != null) {
            digestStream = new DigestOutputStream(cipherStream, digest);
            counterStream = new CountingOutputStream(digestStream);
        } else {
            counterStream = new CountingOutputStream(cipherStream);
        }
    }

    @Override
    public void write(int b) throws IOException {
        counterStream.write(b);
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        counterStream.write(b);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        counterStream.write(b, off, len);
    }
    
    @Override
    public void flush() throws IOException {
        counterStream.flush();
    }
    
    @Override
    public void close() throws IOException {
        if(closed) return;
        closed = true;
        
        counterStream.close();
        
        if(digestStream != null) {
            digest = digestStream.getMessageDigest().digest();
        }
    }

    public byte[] getDigest() {
        if(!closed) {
            throw new IllegalStateException("Cannot call getDigest until stream closed");
        }
        return digest;
    }
    
    public long getByteCount() {
        return counterStream.getByteCount();
    }

}
