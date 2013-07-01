package com.emc.vipr.services.s3.model;

import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessMode;
import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessProtocol;

import java.util.List;

public class GetFileAccessResult {
    private FileAccessMode accessMode;
    private FileAccessProtocol accessProtocol;
    private long fileAccessDuration;
    private List<String> hosts;
    private String user;

    private List<String> mountPoints;
    private boolean isTruncated;
    private List<Object> objects;

    private String lastKey;

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

    public List<String> getMountPoints() {
        return mountPoints;
    }

    public void setMountPoints(List<String> mountPoints) {
        this.mountPoints = mountPoints;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public void setTruncated(boolean truncated) {
        isTruncated = truncated;
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

    /**
     * @return the last key returned by this fileaccess response. if populated, the results in this response are truncated
     */
    public String getLastKey() {
        return lastKey;
    }

    public void setLastKey(String lastKey) {
        this.lastKey = lastKey;
    }
}
