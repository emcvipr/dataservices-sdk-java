package com.emc.vipr.services.s3.model;

public class Object {
    private String name;
    private String deviceExport;
    private String relativePath;
    private String owner;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDeviceExport() {
        return deviceExport;
    }

    public void setDeviceExport(String deviceExport) {
        this.deviceExport = deviceExport;
    }

    public String getRelativePath() {
        return relativePath;
    }

    public void setRelativePath(String relativePath) {
        this.relativePath = relativePath;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }
}
