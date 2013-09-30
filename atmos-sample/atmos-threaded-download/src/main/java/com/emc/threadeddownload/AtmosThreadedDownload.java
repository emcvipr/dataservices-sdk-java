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
package com.emc.threadeddownload;import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.emc.esu.api.Extent;
import com.emc.esu.api.MetadataList;
import com.emc.esu.api.ObjectPath;
import com.emc.esu.api.rest.EsuRestApi;


public class AtmosThreadedDownload implements ProgressListener {

	private String uid;
	private String secret;
	private String host;
	private int port;
	private String file;
	private String localfile;
	private int threads;
	private int blocksize;
	private ObjectPath objectpath;
	private EsuRestApi esu;
	private long filesize;
	private long progress;
	Set<DownloadBlock> blocksRemaining;
	

	public AtmosThreadedDownload(String uid, String secret, String host,
			int port, String file, String localfile, int threads, int blocksize) {
		this.uid = uid;
		this.secret = secret;
		this.host = host;
		this.port = port;
		this.file = file;
		this.localfile = localfile;
		this.threads = threads;
		this.blocksize = blocksize;
	}

	public static void main(String[] args) {
		Options options = new Options();
		Option o = new Option("u", "uid", true, "Atmos UID");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("s", "secret", true, "Atmos Shared Secret");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("h", "host", true, "Atmos Access Point Host");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("p", "port", true, "Atmos Access Point Port (Default 80)");
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option("f", "file", true, "Remote file path");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("l", "localfile", true, "Local file path.  Defaults to same name in current directory");
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option("t", "threads", true, "Thread count.  Defaults to 8");
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option("b", "blocksize", true, "Block size.  Defaults to 4MB");
		o.setRequired(false);
		options.addOption(o);
		
		// create the parser
	    CommandLineParser parser = new GnuParser();
	    try {
	        // parse the command line arguments
	        CommandLine line = parser.parse( options, args );
	        String uid = line.getOptionValue( "uid" );
	        String secret = line.getOptionValue( "secret" );
	        String host = line.getOptionValue( "host" );
	        int port = Integer.parseInt(line.getOptionValue("port", "80"));
	        String file = line.getOptionValue("file");
	        String localfile = line.getOptionValue("localfile");
	        int threads = Integer.parseInt(line.getOptionValue("threads", "8"));
	        int blocksize = Integer.parseInt(line.getOptionValue("blocksize", ""+(4*1024*1024)));
	        
	        AtmosThreadedDownload atd = new AtmosThreadedDownload(uid, secret, host,
	        		port, file, localfile, threads, blocksize);
	        atd.start();
	    }
	    catch( ParseException exp ) {
	        // oops, something went wrong
	        System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
	     // automatically generate the help statement
	        HelpFormatter formatter = new HelpFormatter();
	        formatter.printHelp( "AtmosThreadedDownload", options );
	    }
	}

	private void start() {
		objectpath = new ObjectPath(file);
		if( localfile == null ) {
			localfile = objectpath.getName();
		}
		
		long start, end;
		
		try {
			esu = new EsuRestApi(host, port, uid, secret);
			MetadataList smeta = esu.getSystemMetadata(objectpath, null);
			filesize = Long.parseLong(smeta.getMetadata("size").getValue());
			progress = 0;
			start = System.currentTimeMillis();
			
			File outFile = new File(localfile);
			RandomAccessFile raf = new RandomAccessFile(outFile, "rw");
			raf.setLength(filesize);
			FileChannel channel = raf.getChannel();
	
			long blockcount = filesize/blocksize;
			if( filesize%blocksize != 0 ) {
				blockcount++;
			}
			
			BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
			ThreadPoolExecutor pool = new ThreadPoolExecutor(threads, threads, 15, TimeUnit.SECONDS, queue);
			blocksRemaining = Collections.synchronizedSet(new HashSet<DownloadBlock>());
			
			for(long i=0; i<blockcount; i++) {
				long offset = i*blocksize;
				long size = blocksize;
				if( offset+size > filesize ) {
					size = filesize-offset;
				}
				Extent extent = new Extent(offset, size);
				
				DownloadBlock b = new DownloadBlock();
				b.setChannel(channel);
				b.setEsu(esu);
				b.setExtent(extent);
				b.setListener(this);
				b.setPath(objectpath);
				blocksRemaining.add(b);
				pool.submit(b);
				//System.out.println( "Submitted block " + i );
			}
			
			while( blocksRemaining.size() > 0 ) {
				Thread.sleep(500);
			}
			
			end = System.currentTimeMillis();
			long secs = ((end-start)/1000);
			long rate = filesize / secs;
			System.out.println();
			System.out.println("Downloaded " + filesize + " bytes in " + secs + " seconds (" + rate + " bytes/s)" );
			
			System.exit(0);
		} catch( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

	@Override
	public synchronized void complete(DownloadBlock block) {
		long count = block.getExtent().getSize();
		blocksRemaining.remove(block);
		progress += count;
		long pct = progress*100/filesize;
		System.out.print( "\r" + pct + "% " + progress + "/" + filesize );
		System.out.flush();
	}

	@Override
	public void error(Exception ex) {
		ex.printStackTrace();
		System.exit(2);
	}
}
