package com.emc.vipr.services.s3.model;

public class AppendObjectResult extends UpdateObjectResult {
	private long appendOffset;

	/**
	 * Gets the starting offset inside the object where the data was
	 * appended.
	 * @return the append offset
	 */
	public long getAppendOffset() {
		return appendOffset;
	}

	/**
	 * Sets the starting offset inside the object where the data was
	 * appended.
	 * @param appendOffset the append offset
	 */
	public void setAppendOffset(long appendOffset) {
		this.appendOffset = appendOffset;
	}

}
