package com.emc.vipr.services.s3.model;

import java.util.List;

import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessMode;

public class SetBucketFileAccessModeResult {
	private FileAccessMode accessMode;
	private long fileAccessDuration;
	private List<String> hostList;
	private String user;
	private String token;
	
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
	/**
	 * @return the fileAccessDuration
	 */
	public long getFileAccessDuration() {
		return fileAccessDuration;
	}
	/**
	 * @param fileAccessDuration the fileAccessDuration to set
	 */
	public void setFileAccessDuration(long fileAccessDuration) {
		this.fileAccessDuration = fileAccessDuration;
	}
	/**
	 * @return the hostList
	 */
	public List<String> getHostList() {
		return hostList;
	}
	/**
	 * @param hostList the hostList to set
	 */
	public void setHostList(List<String> hostList) {
		this.hostList = hostList;
	}
	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}
	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}
	/**
	 * @return the token
	 */
	public String getToken() {
		return token;
	}
	/**
	 * @param token the token to set
	 */
	public void setToken(String token) {
		this.token = token;
	}
	
	
}
