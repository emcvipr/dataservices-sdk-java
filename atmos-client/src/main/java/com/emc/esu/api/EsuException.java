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
package com.emc.esu.api;

/**
 * Base ESU exception class that is thrown from the API methods.  Contains an
 * error code that can be mapped to the standard ESU error codes.  See the
 * Atmos programmer's guide for a list of server error codes.
 */
public class EsuException extends RuntimeException {
    private static final long serialVersionUID = -6765742140810819241L;

    private int http_code = 0;      // HTTP return code from Atmos REST API
    private int atmos_code = 0;           // Atmos internal return code

    /**
     * Creates a new ESU Exception with the given message.  The error code
     * will be set to 0.
     * @param message the error message
     */
    public EsuException( String message ) {
        super( message );
    }
    
    /**
     * Creates a new ESU exception with the given message and HTTP error code.
     * @param message the error message
     * @param http_code code the error code
     */
    public EsuException( String message, int http_code ) {
        super( message );
        this.http_code = http_code;
    }

    /**
     * Creates a new ESU exception with the given message and HTTP error code.
     * @param message the error message
     * @param http_code code the error code
     * @param atmos_code detailed code the error code
     */
    public EsuException( String message, int http_code, int atmos_code ) {
        super( message );
        this.http_code = http_code;
        this.atmos_code = atmos_code;
    }
    
    /**
     * Creates a new ESU exception with the given message and cause.
     * @param message the error message
     * @param cause the exception that caused the failure
     */
    public EsuException( String message, Throwable cause ) {
        super( message, cause );
    }
    
    /**
     * Creates a new ESU exception with the given message, code, and cause
     * @param message the error message
     * @param cause the exception that caused the failure
     * @param http_code code the error code
     */
    public EsuException( String message, Throwable cause, int http_code ) {
        super( message, cause );
        this.http_code = http_code;
    }

    /**
     * Creates a new ESU exception with the given message, code, and cause
     * @param message the error message
     * @param cause the exception that caused the failure
     * @param http_code code the error code
     * @param atmos_code detailed code the error code
     */
    public EsuException( String message, Throwable cause, int http_code, int atmos_code ) {
        super( message, cause );
        this.http_code = http_code;
        this.atmos_code = atmos_code;
    }

    /**
     * Returns the HTTP error code associated with the exception.  If unknown
     * (the error did not originate inside the ESU server), the code will be zero.
     * @return the error code
     */
    public int getHttpCode() {
        return http_code;
    }

    /**
     * Returns the Atmos internal error code associated with the exception.
     * If unknown (the error did not originate inside the ESU server), the
     * code will be zero.
     * @return the error code
     */
    public int getAtmosCode() {
        return atmos_code;
    }

    @Override
    public String toString() {
        return "EsuException{" +
               "atmosCode=" + atmos_code +
               ", httpCode=" + http_code +
               "}: " + getMessage();
    }
}
