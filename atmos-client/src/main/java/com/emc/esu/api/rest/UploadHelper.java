/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.esu.api.rest;

import com.emc.esu.api.Acl;
import com.emc.esu.api.BufferSegment;
import com.emc.esu.api.Checksum;
import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.Extent;
import com.emc.esu.api.Identifier;
import com.emc.esu.api.MetadataList;
import com.emc.esu.api.ObjectId;
import com.emc.esu.api.ObjectPath;
import com.emc.esu.api.ProgressListener;
import com.emc.esu.api.Checksum.Algorithm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Helper class to create and update objects.  For large transfers, the content
 * generally needs to be transferred to the server in smaller chunks.  This
 * class reads data from either a file or a stream and incrementally uploads it
 * to the server.  The class also supports the registering of a listener object
 * to report status back to the calling application.
 */
public class UploadHelper {
    private static final Logger l4j = Logger.getLogger(UploadHelper.class);

    public static final int DEFAULT_BUFFSIZE = 4096 * 1024; // 4MB

    private BufferSegment buffer;
    private EsuApi esu;
    private boolean closeStream;
    private InputStream stream;
    private long currentBytes;
    private long totalBytes;
    private boolean complete;
    private boolean failed;
    private Exception error;
    private List<ProgressListener> listeners;
    private int minReadSize = -1;
    private boolean checksumming;
    private Checksum checksum;
    private String mimeType;

    /**
     * Creates a new upload helper.
     * @param esu the API connection object to use to communicate
     * with the server
     * @param buffer the buffer used for making the transfers.  If null, a
     * 4MB buffer will be allocated.
     */
    public UploadHelper(EsuApi esu, byte[] buffer) {
        this.esu = esu;
        if (buffer == null) {
            this.buffer = new BufferSegment(new byte[DEFAULT_BUFFSIZE]);
        } else {
            this.buffer = new BufferSegment(buffer);
        }
        this.listeners = new ArrayList<ProgressListener>();
    }

    /**
     * Creates a new upload helper using a default 4MB buffer.
     * @param api the API connection object to use to communicate
     * with the server
     */
    public UploadHelper(EsuApi api) {
        this(api, null);
    }

    /**
     * Creates a new object on the server with the contents of the given file,
     * acl and metadata.
     * @param f the path to the file to upload
     * @param acl the ACL to assign to the new object.  Optional.  If null,
     * the server will generate a default ACL for the file.
     * @param meta The metadata to assign to the new object.
     * Optional.  If null, no user metadata will be assigned to the new object.
     * @return the identifier of the newly-created object.
     */
    public ObjectId createObject(File f, Acl acl, MetadataList meta) {
        FileInputStream fis;
        // Open the file and call the streaming version
        try {
            totalBytes = f.length();
            fis = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            throw new EsuException("Could not open input file", e);
        }
        return createObject(fis, acl, meta, true);
    }
    
    /**
     * Creates a new object on the server with the contents of the given stream,
     * acl and metadata.
     * @param stream the stream to upload.  The stream will be read until
     * an EOF is encountered.
     * @param acl the ACL to assign to the new object.  Optional.  If null,
     * the server will generate a default ACL for the file.
     * @param metadata The metadata to assign to the new object.
     * Optional.  If null, no user metadata will be assigned to the new object.
     * @param closeStream if true, the stream will be closed after
     * the transfer completes.  If false, the stream will not be closed.
     * @return the identifier of the newly-created object.
     */
    public ObjectId createObject(InputStream stream, Acl acl,
            MetadataList metadata, boolean closeStream) {

        this.currentBytes = 0;
        this.complete = false;
        this.failed = false;
        this.error = null;
        this.closeStream = closeStream;
        this.stream = stream;

        ObjectId id = null;
        
        if( checksumming ) {
        	try {
				checksum = new Checksum( Algorithm.SHA0 );
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException( "Could not initialize checksum", e );
			}
        }

        // First call should be to create object
        try {
            boolean eof = readChunk();
            id = this.esu.createObjectFromSegment(acl, metadata, buffer, mimeType, checksum);
            if (!eof) {
                this.progress(buffer.getSize());
            } else {
                // No data in file? Complete
                this.complete();
                return id;
            }

            // Continue appending
            this.appendChunks(id);

        } catch (EsuException e) {
            this.fail(e);
            throw e;
        } catch (IOException e) {
            this.fail(e);
            throw new EsuException("Error uploading object", e);
        }

        return id;
    }

    /**
     * Creates an object on the server on the given path, file, acl, and metadata.
     * 
     * @param path the path to create the file on
     * @param f the path to the file to upload
     * @param acl the ACL to assign to the new object.  Optional.  If null,
     * the server will generate a default ACL for the file.
     * @param meta The metadata to assign to the new object.
     * Optional.  If null, no user metadata will be assigned to the new object.
     * @return the identifier of the newly-created object.
     */
    public ObjectId createObjectOnPath( ObjectPath path, File f, Acl acl, MetadataList meta ) {
        FileInputStream fis;
        // Open the file and call the streaming version
        try {
            totalBytes = f.length();
            fis = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            throw new EsuException("Could not open input file", e);
        }
        return createObjectOnPath( path, fis, acl, meta, true);
    	
    }

    /**
     * Creates a new object on the server with the contents of the given stream,
     * acl and metadata located at the given path.
     * @param path the object path for the new object.
     * @param stream the stream to upload.  The stream will be read until
     * an EOF is encountered.
     * @param acl the ACL to assign to the new object.  Optional.  If null,
     * the server will generate a default ACL for the file.
     * @param metadata The metadata to assign to the new object.
     * Optional.  If null, no user metadata will be assigned to the new object.
     * @param closeStream if true, the stream will be closed after
     * the transfer completes.  If false, the stream will not be closed.
     * @return the identifier of the newly-created object.
     */
    public ObjectId createObjectOnPath( ObjectPath path, InputStream stream,
			Acl acl, MetadataList metadata, boolean closeStream ) {
        this.currentBytes = 0;
        this.complete = false;
        this.failed = false;
        this.error = null;
        this.closeStream = closeStream;
        this.stream = stream;

        ObjectId id = null;

        if( checksumming ) {
        	try {
				checksum = new Checksum( Algorithm.SHA0 );
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException( "Could not initialize checksum", e );
			}
        }
        // First call should be to create object
        try {
            boolean eof = readChunk();
            id = this.esu.createObjectFromSegmentOnPath(path, acl, metadata, buffer, mimeType, checksum);
            if (!eof) {
                this.progress(buffer.getSize());
            } else {
                // No data in file? Complete
                this.complete();
                return id;
            }

            // Continue appending
            this.appendChunks( path );

        } catch (EsuException e) {
            this.fail(e);
            throw e;
        } catch (IOException e) {
            this.fail(e);
            throw new EsuException("Error uploading object", e);
        }

        return id;
	}

	/**
     * Updates an existing object with the contents of the given file, ACL, and
     * metadata.
     * @param id the identifier of the object to update.
     * @param f the path to the file to replace the object's current
     * contents with
     * @param acl the ACL to update the object with.  Optional.  If null,
     * the ACL will not be modified.
     * @param metadata The metadata to assign to the object.
     * Optional.  If null, no user metadata will be modified.
     */
    public void updateObject(Identifier id, File f, Acl acl, MetadataList metadata) {
        // Open the file and call the streaming version
        InputStream in;
        try {
            in = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            throw new EsuException("Could not open input file", e);
        }
        totalBytes = f.length();
        updateObject(id, in, acl, metadata, true);
    }

    /**
     * Updates an existing object with the contents of the given stream, ACL, and
     * metadata.
     * @param id the identifier of the object to update.
     * @param stream the stream to replace the object's current
     * contents with.  The stream will be read until an EOF is encountered.
     * @param acl the ACL to update the object with.  Optional.  If not
     * specified, the ACL will not be modified.
     * @param metadata The metadata to assign to the object.
     * Optional.  If null, no user metadata will be modified.
     */
    public void updateObject(Identifier id, InputStream stream, Acl acl,
            MetadataList metadata, boolean closeStream) {

        this.currentBytes = 0;
        this.complete = false;
        this.failed = false;
        this.error = null;
        this.closeStream = closeStream;
        this.stream = stream;

        if( checksumming ) {
        	try {
				checksum = new Checksum( Algorithm.SHA0 );
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException( "Could not initialize checksum", e );
			}
        }

        // First call uses a null extent to truncate the file.
        try {
            boolean eof = readChunk();
            this.esu.updateObjectFromSegment(id, acl, metadata, null, buffer,
                    mimeType, checksum);

            if (!eof) {
                this.progress(buffer.getSize());
            } else {
                // No data in file? Complete
                this.complete();
                return;
            }

            // Continue appending
            this.appendChunks(id);

        } catch (EsuException e) {
            this.fail(e);
            throw e;
        } catch (IOException e) {
            this.fail(e);
            throw new EsuException("Error updating object", e);
        }

    }

    /**
     * Adds the given progress listener to the listener list.
     * @param l the listener to add
     */
    public void addListener(ProgressListener l) {
        listeners.add(l);
    }

    /**
     * Removes the given progress listener from the listener list.
     * @param l the listener to remove
     */
    public void removeListener(ProgressListener l) {
        listeners.remove(l);
    }

    /**
     * Returns the total bytes in the request.  If unknown, -1 is returned.
     * @return the totalBytes
     */
    public long getTotalBytes() {
        return totalBytes;
    }

    /**
     * Sets the total number of bytes to transfer in the upload
     * @param totalBytes the totalBytes to set
     */
    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    /**
     * Returns the current number of bytes uploaded.
     * @return the currentBytes
     */
    public long getCurrentBytes() {
        return currentBytes;
    }

    /**
     * Returns true if the upload request has completed.
     * @return true if the upload is complete
     */
    public boolean isComplete() {
        return complete;
    }

    /**
     * Returns true if the upload request has failed.
     * @return true if the upload has failed
     */
    public boolean isFailed() {
        return failed;
    }

    /**
     * Returns the Exception that caused the upload to fail.
     * @return the Exception or null if the upload has not failed.
     */
    public Exception getError() {
        return error;
    }

    /////////////////////
    // Private Methods //
    /////////////////////

    /**
     * Continues writing data to the object until EOF
     * @throws IOException 
     */
    private void appendChunks(Identifier id) throws IOException {
        while (true) {
            boolean eof = readChunk();
            if (eof) {
                // done
                complete();
                return;
            }

            Extent extent = new Extent(currentBytes, buffer.getSize());
            esu.updateObjectFromSegment(id, null, null, extent, buffer, null, checksum);
            this.progress(buffer.getSize());
        }

    }

    /**
     * Fails the upload and notifies the listeners.
     * @param e exception that caused the failure.
     */
    private void fail(Exception e) {
        failed = true;
        error = e;
        if (closeStream) {
            try {
                stream.close();
            } catch (IOException e1) {
                // ignore
                l4j.warn("Error closing stream", e1);
            }
        }
        for (Iterator<ProgressListener> i = listeners.iterator(); i.hasNext();) {
            ProgressListener pl = i.next();
            pl.onError(e);
        }
    }

    /**
     * Marks the upload as completed and notifies the listeners.
     */
    private void complete() {
        complete = true;

        if (closeStream) {
            try {
                stream.close();
            } catch (IOException e) {
                // ignore
                l4j.warn("Error closing stream", e);
            }
        }
        for (Iterator<ProgressListener> i = listeners.iterator(); i.hasNext();) {
            ProgressListener pl = i.next();
            pl.onComplete();
        }
    }

    /**
     * Notifies the listeners of upload progress.
     * @param size
     */
    private void progress(int size) {
        currentBytes += size;
        for (Iterator<ProgressListener> i = listeners.iterator(); i.hasNext();) {
            ProgressListener pl = i.next();
            pl.onProgress(currentBytes, totalBytes);
        }
    }

    /**
     * Reads a chunk of data from the stream.
     * @return true if an EOF was encountered.
     */
    private boolean readChunk() throws IOException {
    	if( minReadSize == -1 ) {
	        int c = stream.read(buffer.getBuffer());
	        if (c == -1) {
	            buffer.setSize(0);
	            return true;
	        }
	        buffer.setSize(c);
	        return false;
    	} else {
    		// Some stream implementations return small chunks (network streams for instance)
    		// this can cause a lot of overhead making many small requests to Atmos.  If set,
    		// we can make repeated reads to ensure a buffer has size before communicating with
    		// atmos.
    		int read = 0;
    		while( read < minReadSize ) {
    	        int c = stream.read(buffer.getBuffer(), read, buffer.getSize()-read);
    	        if (c == -1) {
    	        	// EOF encountered.
    	        	if( read > 0 ) {
    	        		buffer.setSize( read );
    	        		return false;
    	        	} else {
    	        		buffer.setSize(0);
    	        		return true;
    	        	}
    	        }
    	        read += c;
    		}
	        buffer.setSize(read);
	        return false;
    	}
    }

	public void setMinReadSize(int minReadSize) {
		this.minReadSize = minReadSize;
	}

	public int getMinReadSize() {
		return minReadSize;
	}

	/**
	 * @return the checksumming
	 */
	public boolean isChecksumming() {
		return checksumming;
	}

	/**
	 * @param checksumming the checksumming to set
	 */
	public void setChecksumming(boolean checksumming) {
		this.checksumming = checksumming;
	}

	/**
	 * Sets the mimetype to be used when creating the object.
	 * @param mimeType the mimeType
	 */
	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	/**
	 * @return the mimeType
	 */
	public String getMimeType() {
		return mimeType;
	}

}
