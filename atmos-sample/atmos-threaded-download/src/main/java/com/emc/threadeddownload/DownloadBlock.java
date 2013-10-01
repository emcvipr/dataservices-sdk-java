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
package com.emc.threadeddownload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.Extent;
import com.emc.esu.api.ObjectPath;

public class DownloadBlock implements Runnable {
	private Extent extent;
	public Extent getExtent() {
		return extent;
	}

	public void setExtent(Extent extent) {
		this.extent = extent;
	}

	public FileChannel getChannel() {
		return channel;
	}

	public void setChannel(FileChannel channel) {
		this.channel = channel;
	}

	public EsuApi getEsu() {
		return esu;
	}

	public void setEsu(EsuApi esu) {
		this.esu = esu;
	}

	public ObjectPath getPath() {
		return path;
	}

	public void setPath(ObjectPath path) {
		this.path = path;
	}

	public ProgressListener getListener() {
		return listener;
	}

	public void setListener(ProgressListener listener) {
		this.listener = listener;
	}

	private FileChannel channel;
	private EsuApi esu;
	private ObjectPath path;
	private ProgressListener listener;

	@Override
	public void run() {
		//System.err.println( "Thread: " + Thread.currentThread() + " read: " + extent);
		
		try {
			byte[] data = null;
			while(true) {
				try {
					data = esu.readObject(path, extent, null);
					break;
				} catch(EsuException e) {
					System.err.println( "Read failed: " + e + "( " + e.getCause() + "), retrying" );
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			//System.err.println( "Thread: " + Thread.currentThread() + " write: " + extent);
			//System.out.flush();
			channel.write(ByteBuffer.wrap(data), extent.getOffset());
			//System.err.println( "Thread: " + Thread.currentThread() + " complete: " + extent);
			//System.out.flush();
			listener.complete(this);
		} catch (IOException e) {
			listener.error(e);
		}
		
	}

}
