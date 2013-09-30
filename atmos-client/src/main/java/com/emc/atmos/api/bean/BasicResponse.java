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
package com.emc.atmos.api.bean;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlTransient;
import java.util.Date;
import java.util.List;
import java.util.Map;

@XmlTransient
@XmlAccessorType( XmlAccessType.NONE )
public class BasicResponse {
    protected int httpStatus;
    protected String httpMessage;
    protected Map<String, List<String>> headers;
    protected String contentType;
    protected long contentLength;
    protected String location;
    protected Date lastModified;
    protected Date date;

    public Date getDate() {
        return date;
    }

    public void setDate( Date date ) {
        this.date = date;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus( int httpStatus ) {
        this.httpStatus = httpStatus;
    }

    public String getHttpMessage() {
        return httpMessage;
    }

    public void setHttpMessage( String httpMessage ) {
        this.httpMessage = httpMessage;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public void setHeaders( Map<String, List<String>> headers ) {
        this.headers = headers;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType( String contentType ) {
        this.contentType = contentType;
    }

    public long getContentLength() {
        return contentLength;
    }

    public void setContentLength( long contentLength ) {
        this.contentLength = contentLength;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation( String location ) {
        this.location = location;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified( Date lastModified ) {
        this.lastModified = lastModified;
    }

    protected String getFirstHeader( String headerName ) {
        if ( headers != null ) {
            List<String> values = headers.get( headerName );
            if ( values != null && !values.isEmpty() ) {
                return values.get( 0 );
            }
        }
        return null;
    }
}
