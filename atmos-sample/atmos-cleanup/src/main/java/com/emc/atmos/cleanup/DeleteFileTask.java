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

import org.apache.log4j.Logger;

import com.emc.esu.api.EsuException;
import com.emc.esu.api.ObjectPath;

public class DeleteFileTask extends TaskNode {
	private static final Logger l4j = Logger.getLogger(DeleteFileTask.class);
	
	private ObjectPath filePath;
	private AtmosCleanup cleanup;

	@Override
	protected TaskResult execute() throws Exception {
		//l4j.debug("Deleting file " + filePath);
		try {
			cleanup.getEsu().deleteObject(filePath);
		} catch(EsuException e) {
			cleanup.failure(this, filePath, e);
			return new TaskResult(false);
		}

		cleanup.success(this, filePath);
		return new TaskResult(true);
	}

	public ObjectPath getFilePath() {
		return filePath;
	}

	public void setFilePath(ObjectPath filePath) {
		this.filePath = filePath;
	}

	public AtmosCleanup getCleanup() {
		return cleanup;
	}

	public void setCleanup(AtmosCleanup cleanup) {
		this.cleanup = cleanup;
	}

	@Override
	public String toString() {
		return "DeleteFileTask [filePath=" + filePath + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((filePath == null) ? 0 : filePath.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DeleteFileTask other = (DeleteFileTask) obj;
		if (filePath == null) {
			if (other.filePath != null)
				return false;
		} else if (!filePath.equals(other.filePath))
			return false;
		return true;
	}

}
