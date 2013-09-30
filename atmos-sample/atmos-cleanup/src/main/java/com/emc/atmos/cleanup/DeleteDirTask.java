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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.emc.esu.api.DirectoryEntry;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.ListOptions;
import com.emc.esu.api.ObjectPath;

public class DeleteDirTask extends TaskNode {
	private static final Logger l4j = Logger.getLogger(DeleteDirTask.class);
	
	private ObjectPath dirPath;
	private AtmosCleanup cleanup;
	private Set<DeleteDirChild> parentDirs = new HashSet<DeleteDirChild>();

	@Override
	protected TaskResult execute() throws Exception {
		//l4j.debug( "Descending into " + dirPath );
		
		try {
		
			// All entries in this directory will become parents of the child task
			// that deletes the current directory after it's empty.
			DeleteDirChild child = new DeleteDirChild();
			child.addParent(this);
			child.addToGraph(cleanup.getGraph());
			for(DeleteDirChild ch : parentDirs) {
				ch.addParent(child);
			}
			
			ListOptions options = new ListOptions();
			List<DirectoryEntry> ents = cleanup.getEsu().listDirectory(dirPath, options);
			while( options.getToken() != null ) {
				l4j.debug( "Continuing " + dirPath + " on token " + options.getToken() );
				ents.addAll( cleanup.getEsu().listDirectory( dirPath, options ) );
			}
			
			for( DirectoryEntry ent : ents ) {
				if( ent.getPath().toString().equals( "/apache/" ) ) {
					// Skip listable tags dir
					continue;
				}
				if( "directory".equals( ent.getType() ) ) {
					DeleteDirTask ddt = new DeleteDirTask();
					ddt.setDirPath(ent.getPath());
					ddt.setCleanup(cleanup);
					ddt.addParent(this);
					
					
					cleanup.increment(ent.getPath());
					ddt.addToGraph(cleanup.getGraph());
					child.addParent(ddt);
					// If we have any other children to depend on, add them
					for(DeleteDirChild ch : parentDirs) {
						ch.addParent(ddt);
						ddt.parentDirs.add(ch);
					}
					ddt.parentDirs.add(child);
				} else {
					DeleteFileTask dft = new DeleteFileTask();
					dft.setFilePath(ent.getPath());
					dft.setCleanup(cleanup);
					dft.addParent(this);
					cleanup.increment(ent.getPath());
					dft.addToGraph(cleanup.getGraph());
					child.addParent(dft);
				}
			}
			
		} catch(Exception e) {
			cleanup.failure(this, dirPath, e);
			return new TaskResult(false);
		}
		return new TaskResult(true);
	}

	public ObjectPath getDirPath() {
		return dirPath;
	}

	public void setDirPath(ObjectPath dirPath) {
		this.dirPath = dirPath;
	}

	public AtmosCleanup getCleanup() {
		return cleanup;
	}

	public void setCleanup(AtmosCleanup cleanup) {
		this.cleanup = cleanup;
	}
	
	
	/**
	 * You can't delete the directory until all the children are deleted, so
	 * this task will depend on all the children before delete.
	 */
	public class DeleteDirChild extends TaskNode {
	    public ObjectPath getDirPath() {
	        return dirPath;
	    }

		@Override
		protected TaskResult execute() throws Exception {
			if( !dirPath.toString().equals("/") ) {
				// Delete directory
				try {
					cleanup.getEsu().deleteObject( dirPath );
				} catch(EsuException e) {
					cleanup.failure(this, dirPath, e);
					return new TaskResult(false);
				}
			}
			cleanup.success(this, dirPath);

			return new TaskResult(true);
		}

		
		@Override
		public String toString() {
			return "DeleteDirTask$DeleteDirChild [dirPath=" + dirPath + "]";
		}
		
		@Override
		public boolean equals(Object obj) {
		    if(!(obj instanceof DeleteDirChild)) {
		        return false;
		    }
		    return dirPath.equals(((DeleteDirChild)obj).getDirPath());
		}
		
		@Override
		public int hashCode() {
		    return dirPath.hashCode();
		}
	}


	@Override
	public String toString() {
		return "DeleteDirTask [dirPath=" + dirPath + ", parentDirs="
				+ parentDirs + "]";
	}

	@Override
	public int hashCode() {
		return dirPath.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DeleteDirTask other = (DeleteDirTask) obj;
		if (dirPath == null) {
			if (other.dirPath != null)
				return false;
		} else if (!dirPath.equals(other.dirPath))
			return false;
		return true;
	}


}
