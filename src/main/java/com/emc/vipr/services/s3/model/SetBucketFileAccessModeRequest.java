package com.emc.vipr.services.s3.model;

import com.amazonaws.AmazonWebServiceRequest;
import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessMode;
import com.emc.vipr.services.s3.model.ViPRConstants.FileAccessProtocol;

import java.util.Arrays;
import java.util.List;

public class SetBucketFileAccessModeRequest extends AmazonWebServiceRequest {
    private String bucketName;
    private FileAccessMode accessMode;
    private FileAccessProtocol accessProtocol;
    private long fileAccessDuration = -1;
    private List<String> hostList;
    private String user;
    private String token;

    private static final FileAccessMode[] ALLOWED_ACCESS_MODES = {
            FileAccessMode.Disabled, FileAccessMode.ReadOnly, FileAccessMode.ReadWrite};

    /**
     * @return the accessMode
     */
    public FileAccessMode getAccessMode() {
        return accessMode;
    }

    /**
     * @param accessMode the accessMode to set
     */
    public void setAccessMode( FileAccessMode accessMode ) {
        if ( !Arrays.asList( ALLOWED_ACCESS_MODES ).contains( accessMode ) )
            throw new IllegalArgumentException( "Access mode must be one of "
                                                + Arrays.toString( ALLOWED_ACCESS_MODES ) );
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
    public void setAccessProtocol( FileAccessProtocol accessProtocol ) {
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
    public void setFileAccessDuration( long fileAccessDuration ) {
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
    public void setHostList( List<String> hostList ) {
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
    public void setUser( String user ) {
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
    public void setToken( String token ) {
        this.token = token;
    }

    /**
     * @return the bucketName
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * @param bucketName the bucketName to set
     */
    public void setBucketName( String bucketName ) {
        this.bucketName = bucketName;
    }
}
