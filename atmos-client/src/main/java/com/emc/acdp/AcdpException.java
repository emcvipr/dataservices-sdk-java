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
package com.emc.acdp;

public class AcdpException extends RuntimeException {
    private int httpCode = 0;
    private String acdpCode;

    public AcdpException( String message ) {
        this( message, 0, null );
    }

    public AcdpException( String message, int httpCode ) {
        this( message, httpCode, null );
    }

    public AcdpException( String message, int httpCode, String acdpCode ) {
        super( message );
        this.httpCode = httpCode;
        this.acdpCode = acdpCode;
    }

    public AcdpException( String message, Throwable throwable ) {
        this( message, throwable, 0, null );
    }

    public AcdpException( String message, Throwable throwable, int httpCode ) {
        this( message, throwable, httpCode, null );
    }

    public AcdpException( String message, Throwable throwable, int httpCode, String acdpCode ) {
        super( message, throwable );
        this.httpCode = httpCode;
        this.acdpCode = acdpCode;
    }

    public int getHttpCode() {
        return httpCode;
    }

    public String getAcdpCode() {
        return acdpCode;
    }
}
