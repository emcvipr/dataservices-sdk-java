/**
 * 
 */
package com.emc.vipr.transform.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import SevenZip.Compression.LZMA.Decoder;

/**
 * @author cwikj
 *
 */
public class LZMAInputStream extends InputStream implements Runnable {
    private static ThreadGroup tg = new ThreadGroup("LZMA Decompress");
    private InputStream compressedStream;
    private Thread decompressionThread;
    private PipedOutputStream uncompressedPipeOut;
    private PipedInputStream uncompressedPipeIn;
    boolean closed;
    private Exception decompressionException;
    private Decoder lzma;
    private Boolean decompressionComplete;

    /**
     * @throws IOException 
     * 
     */
    public LZMAInputStream(InputStream compressedStream) throws IOException {
        this(compressedStream, 4096);
    }
    
    public LZMAInputStream(InputStream compressedStream, int bufferSize) throws IOException {
        this.compressedStream = compressedStream;
        
        lzma = new Decoder();
        
        // Read the stream properties from the stream
        byte[] properties = new byte[5];
        int c = compressedStream.read(properties);
        if(c != properties.length) {
            throw new IOException("Unable to compression settings from stream");
        }
        
        if(!lzma.SetDecoderProperties(properties)) {
            throw new IOException("LZMA decoder rejected compression settings from stream");
        }
        
        // Build a pipe to read data from the decoder.
        uncompressedPipeIn = new PipedInputStream(bufferSize);
        uncompressedPipeOut = new PipedOutputStream(uncompressedPipeIn);

        // Start the auxilary thread to do the decompression.
        decompressionThread = new Thread(tg, this);
        closed = false;
        setDecompressionComplete(Boolean.FALSE);
        decompressionThread.start();
        
    }

    /* (non-Javadoc)
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        checkStream();
        try {
            return uncompressedPipeIn.read();
        } catch(IOException e) {
            if(getDecompressionComplete()) {
                return -1;
            }
            throw e;
        }
    }
    

    @Override
    public int available() throws IOException {
        return uncompressedPipeIn.available();
    }

    @Override
    public void close() throws IOException {
        if(closed) {
            return;
        }
        
        // Close the parent stream
        compressedStream.close();
        
        // Wait for decompression to end
        try {
            decompressionThread.join();
        } catch (InterruptedException e) {
            // Ignore.
        }
        
        // Close the pipe ends.
        uncompressedPipeIn.close();
        uncompressedPipeOut.close();
        
        // dereference the decoder so it can get collected asap.
        lzma = null;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkStream();
        try {
            return uncompressedPipeIn.read(b, off, len);
        } catch(IOException e) {
            if(getDecompressionComplete()) {
                return -1;
            }
            throw e;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        checkStream();
        try {
            return uncompressedPipeIn.read(b);
        } catch(IOException e) {
            if(getDecompressionComplete()) {
                return -1;
            }
            throw e;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        checkStream();
        try {
            return uncompressedPipeIn.skip(n);
        } catch(IOException e) {
            if(getDecompressionComplete()) {
                return -1;
            }
            throw e;
        }
    }

    private void checkStream() throws IOException {
        if(decompressionException != null) {
            throw new IOException("Error decompressing data", decompressionException);
        }
        if(closed) {
            throw new IOException("Stream closed");
        }
    }

    @Override
    public void run() {
        try {
            lzma.Code(compressedStream, uncompressedPipeOut, -1);
            // Tell the pipe we're done
            uncompressedPipeOut.close();
        } catch(Exception e) {
            decompressionException = e;
        }
        setDecompressionComplete(Boolean.TRUE);
    }

    private synchronized Boolean getDecompressionComplete() {
        return decompressionComplete;
    }

    private synchronized void setDecompressionComplete(Boolean decompressionComplete) {
        this.decompressionComplete = decompressionComplete;
    }


}
