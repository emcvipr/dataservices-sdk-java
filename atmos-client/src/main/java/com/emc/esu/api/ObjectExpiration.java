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

import java.util.Date;
import java.util.List;

import org.jdom.Element;
import org.jdom.Namespace;

/**
 * Encapsulates the object's expriation information
 */
public class ObjectExpiration {
	private boolean enabled;
	private Date endAt;
	
	public ObjectExpiration() {
	}
	
	@SuppressWarnings("rawtypes")
	public ObjectExpiration(Element element) {
        Namespace esuNs = Namespace.getNamespace( "http://www.emc.com/cos/" );
        
        // Parse Enabled flag
        List children = element.getChildren( "enabled", esuNs );
        if( children == null || children.size() < 1 ) {
        	throw new EsuException( "id not found in replica" );
        }
        enabled = "true".equals(((Element)children.get(0)).getTextTrim());
        
        children = element.getChildren( "endAt", esuNs );
        if( children == null || children.size() < 1 ) {
        	throw new EsuException( "endAt not found in replica" );
        }
        endAt = ObjectId.parseXmlDate(((Element)children.get(0)).getTextTrim());
	}
	/**
	 * @return the enabled
	 */
	public boolean isEnabled() {
		return enabled;
	}
	/**
	 * @param enabled the enabled to set
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	/**
	 * @return the endAt
	 */
	public Date getEndAt() {
		return endAt;
	}
	/**
	 * @param endAt the endAt to set
	 */
	public void setEndAt(Date endAt) {
		this.endAt = endAt;
	}
}
