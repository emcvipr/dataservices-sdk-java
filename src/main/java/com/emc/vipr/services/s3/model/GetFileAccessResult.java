package com.emc.vipr.services.s3.model;

import java.util.List;

import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessMode;
import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessProtocol;

public class GetFileAccessResult {
	private FileAccessMode accessMode;
	private FileAccessProtocol accessProtocol;
	private long fileAccessDuration;
	private String marker;
	private boolean truncated;
	private List<String> hosts;
	private String user;
	
	private List<MountPoint> mountPoints;
	private List<Object> objects;
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
	 * @return the accessProtocol
	 */
	public FileAccessProtocol getAccessProtocol() {
		return accessProtocol;
	}
	/**
	 * @param accessProtocol the accessProtocol to set
	 */
	public void setAccessProtocol(FileAccessProtocol accessProtocol) {
		this.accessProtocol = accessProtocol;
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
	 * @return the marker
	 */
	public String getMarker() {
		return marker;
	}
	/**
	 * @param marker the marker to set
	 */
	public void setMarker(String marker) {
		this.marker = marker;
	}
	/**
	 * @return the truncated
	 */
	public boolean isTruncated() {
		return truncated;
	}
	/**
	 * @param truncated the truncated to set
	 */
	public void setTruncated(boolean truncated) {
		this.truncated = truncated;
	}
	/**
	 * @return the hosts
	 */
	public List<String> getHosts() {
		return hosts;
	}
	/**
	 * @param hosts the hosts to set
	 */
	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
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
	 * @return the mountPoints
	 */
	public List<MountPoint> getMountPoints() {
		return mountPoints;
	}
	/**
	 * @param mountPoints the mountPoints to set
	 */
	public void setMountPoints(List<MountPoint> mountPoints) {
		this.mountPoints = mountPoints;
	}
	/**
	 * @return the objects
	 */
	public List<Object> getObjects() {
		return objects;
	}
	/**
	 * @param objects the objects to set
	 */
	public void setObjects(List<Object> objects) {
		this.objects = objects;
	}
	
	
}
