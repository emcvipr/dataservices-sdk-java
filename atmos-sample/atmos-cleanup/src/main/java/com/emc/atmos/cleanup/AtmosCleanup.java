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
package com.emc.atmos.cleanup;

import java.io.File;
import java.util.Arrays;
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
import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.ObjectPath;
import com.emc.esu.api.rest.LBEsuRestApiApache;

/**
 * Recursively deletes objects in the Atmos namespace (like rm -r).
 * @author cwikj
 *
 */
public class AtmosCleanup {
	private static final Logger l4j = Logger.getLogger(AtmosCleanup.class);
	private String remoteroot;
	private EsuApi esu;
	private int dirCount;
	private int fileCount;
	private SimpleDirectedGraph<com.emc.atmos.cleanup.TaskNode, DefaultEdge> graph;
	private Set<com.emc.atmos.cleanup.TaskNode> failedItems;
	private BlockingQueue<Runnable> queue;
	private int threads;
	private ThreadPoolExecutor pool;
	private String[] hosts;
	private int port;
	private String uid;
	private String secret;
	private int failedCount;
	private int completedCount;
	
	public AtmosCleanup() {
		dirCount = 0;
		fileCount = 0;
		failedCount = 0;
		completedCount = 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Options options = new Options();
		Option o = new Option("u", "uid", true, "Atmos UID in the form of subtenantid/uid, e.g. 640f9a5cc636423fbc748566b397d1e1/uid1");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("s", "secret", true, "Atmos Shared Secret");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("h", "host", true,
				"Atmos Access Point Host(s). You can repeat this option to load balance across multiple Atmos nodes.");
		o.setRequired(true);
		o.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(o);

		o = new Option("p", "port", true,
				"Atmos Access Point Port (Default 80)");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("r", "remoteroot", true, "Remote root path (e.g. \"/\")");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("t", "threads", true, "Thread count.  Defaults to 8");
		o.setRequired(false);
		options.addOption(o);

		// create the parser
		CommandLineParser parser = new GnuParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			String uid = line.getOptionValue("uid");
			String secret = line.getOptionValue("secret");
			String[] hosts = line.getOptionValues("host");
			int port = Integer.parseInt(line.getOptionValue("port", "80"));
			String remoteroot = line.getOptionValue("remoteroot");
			int threads = Integer.parseInt(line.getOptionValue("threads", "8"));

			AtmosCleanup cleanup = new AtmosCleanup();
			cleanup.setUid(uid);
			cleanup.setSecret(secret);
			cleanup.setHosts(hosts);
			cleanup.setPort(port);
			cleanup.setRemoteroot(remoteroot);
			cleanup.setThreads(threads);
			
			cleanup.start();
		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar AtmosCleanup.jar <options ...>", options);
		} 
		
		System.exit(0);
	}

	private void start() {
		// Make sure remote path is in the correct format
		if( !remoteroot.startsWith( "/" ) ) {
			remoteroot = "/" + remoteroot;
		}
		if( !remoteroot.endsWith( "/" ) ) {
			// Must be a dir (ends with /)
			remoteroot = remoteroot + "/";
		}
		
		this.esu = new LBEsuRestApiApache(Arrays.asList(hosts), port, uid, secret);
		
		// Test connection to server
		try {
			String version = esu.getServiceInformation().getAtmosVersion();
			l4j.info( "Connected to atmos " + version + " on host(s) " + Arrays.asList(hosts) );
		} catch( Exception e ) {
			l4j.error( "Error connecting to server: " + e, e );
			System.exit( 3 );
		}
		
		ObjectPath op = new ObjectPath( remoteroot );
		
		queue = new LinkedBlockingQueue<Runnable>();
		pool = new ThreadPoolExecutor(threads, threads, 15, TimeUnit.SECONDS, queue);
		failedItems = Collections.synchronizedSet( new HashSet<TaskNode>() );
		
		graph = new SimpleDirectedGraph<TaskNode, DefaultEdge>(DefaultEdge.class);

		long start = System.currentTimeMillis();
		
		DeleteDirTask ddt = new DeleteDirTask();
		ddt.setDirPath(op);
		ddt.setCleanup(this);
		increment(op);
		ddt.addToGraph(graph);
		
		while(true) {
			synchronized (graph) {
				//l4j.debug("Vertexes: " + graph.vertexSet().size());
				if( graph.vertexSet().size() == 0 ) {
					// We're done
					pool.shutdownNow();
					break;
				}
				
				if(pool.getQueue().size() < threads*5) {
	                // Look for available unsubmitted tasks
	                BreadthFirstIterator<TaskNode, DefaultEdge> i = new BreadthFirstIterator<TaskNode, DefaultEdge>(graph);
	                while( i.hasNext() ) {
	                    TaskNode t = i.next();
	                    if( graph.inDegreeOf(t) == 0 && !t.isQueued() ) {
	                        t.setQueued(true);
	                        //l4j.debug( "Submitting " + t );
	                        pool.submit(t);
	                    }
	                }
				}
				
			}
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// Ignore
			}
		}
		
		long end = System.currentTimeMillis();
		long secs = ((end-start)/1000);
		if( secs == 0 ) {
			secs = 1;
		}
		
		long rate = (fileCount+dirCount) / secs;
		System.out.println("Deleted " + (fileCount+dirCount) + " objects in " + secs + " seconds (" + rate + " obj/s)" );
		System.out.println("Files: " + fileCount + " Directories: " + dirCount + " Failed Objects: " + failedCount );
		System.out.println("Failed Files: " + failedItems );
		
		if( failedCount > 0 ) {
			System.exit(1);
		} else {
			System.exit(0);
		}
		
		System.exit( 0 );
	}

	
	
	public synchronized void failure(TaskNode task, ObjectPath objectPath,
			Exception e) {
		
		if( e instanceof EsuException ) {
			l4j.error( "Failed to delete " + objectPath + ": " + e + " code: " + ((EsuException)e).getAtmosCode(), e );
			
		} else {
			l4j.error( "Failed to delete " + objectPath + ": " + e, e );
		}
		failedCount++;
		failedItems.add( task );

	}


	public String getRemoteroot() {
		return remoteroot;
	}

	public void setRemoteroot(String remoteroot) {
		this.remoteroot = remoteroot;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public String[] getHosts() {
		return hosts;
	}

	public void setHosts(String[] hosts) {
		this.hosts = hosts;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getSecret() {
		return secret;
	}

	public void setSecret(String secret) {
		this.secret = secret;
	}

	public EsuApi getEsu() {
		return esu;
	}
	
	public synchronized void increment(ObjectPath objectPath) {
		if(objectPath.isDirectory()) {
			dirCount++;
		} else {
			fileCount++;
		}
	}

	public synchronized void success(TaskNode task, ObjectPath objectPath) {
		
		completedCount++;
		int pct = completedCount*100 / (fileCount+dirCount);
		l4j.info( pct + "% (" + completedCount + "/" + (fileCount+dirCount) +") Completed: " + objectPath );
		
	}

	public SimpleDirectedGraph<TaskNode, DefaultEdge> getGraph() {
		return graph;
	}

}
