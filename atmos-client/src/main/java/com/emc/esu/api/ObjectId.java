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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Encapsulates a ESU object identifier.  Performs validation upon construction
 * to ensure that the identifier format is valid.  
 */
public class ObjectId implements Identifier {
    /**
     * Regular expression used to validate identifiers.
     */
    private static final Pattern ID_FORMAT = Pattern.compile( "^[0-9a-f]{44}$" );
    
    /**
     * Stores the string representation of the identifier
     */
    private String id;

    /**
     * Constructs a new object identifier
     * @param id the object ID as a string
     */
    public ObjectId( String id ) {
        if( !ID_FORMAT.matcher( id ).matches() ) {
            throw new EsuException( id + " is not a valid object id" );
        }
        this.id = id;
    }
    
    /**
     * Returns the identifier as a string
     * @return the identifier as a string
     */
    public String toString() {
        return id;
    }
    
    /**
     * Returns true if the object IDs are equal.
     */
    public boolean equals( Object obj ) {
    	if( obj instanceof ObjectResult ) {
    		return this.equals( ((ObjectResult)obj).getId() );
    	}
        if( !(obj instanceof ObjectId) ) {
            return false;
        }
        
        return id.equals( ((ObjectId)obj).toString() );
        
    }
    
    /**
     * Returns a hash code for this object id.
     */
    public int hashCode() {
        return id.hashCode();
    }

	public static Date parseXmlDate(String dateText) {
		if( dateText == null || dateText.length() < 1 ) {
			return null;
		}
		
		DateFormat xmlDate = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
		xmlDate.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
		
		try {
			Date d = xmlDate.parse( dateText );
			return d;
		} catch (ParseException e) {
			throw new EsuException( "Failed to parse date: " + dateText, e );
		}
	}
}
