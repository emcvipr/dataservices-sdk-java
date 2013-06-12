package com.emc.vipr.services.s3.model;

import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessMode;

public class GetBucketFileAccessModeResult {
	private FileAccessMode accessMode;

	/**
	 * @return the accessMode
	 */
	public FileAccessMode getAccessMode() {
		return accessMode;
	}

	/**
	 * @param accessMode the accessMode to set
	 */
	public void setAccessMode(FileAccessMode accessMode) {
		this.accessMode = accessMode;
	}
	
}
