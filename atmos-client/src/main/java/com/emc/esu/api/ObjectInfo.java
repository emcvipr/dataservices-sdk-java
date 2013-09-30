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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

/**
 * Encapsulates the information from the ObjectInfo call.  Contains replica,
 * retention, and expiration information.
 */
public class ObjectInfo {
	private String rawXml;
	private ObjectId objectId;
	private String selection;
	private List<ObjectReplica> replicas;
	private ObjectRetention retention;
	private ObjectExpiration expiration;
	
	public ObjectInfo() {
		replicas = new ArrayList<ObjectReplica>();
	}
	
	public ObjectInfo(String xml) {
		replicas = new ArrayList<ObjectReplica>();
		rawXml = xml;
		parse(xml);
	}

	@SuppressWarnings("rawtypes")
	public void parse(String xml) {
        // Use JDOM to parse the XML
        SAXBuilder sb = new SAXBuilder();
        try {
            Document d = sb.build( new StringReader(xml) );
            
            // The elements are part of a namespace so we need to use
            // the namespace to identify the elements.
            Namespace esuNs = Namespace.getNamespace( "http://www.emc.com/cos/" );
            
            List children = d.getRootElement().getChildren( "objectId", esuNs );
            if( children == null || children.size() < 1 ) {
            	throw new EsuException( "objectId not found in response" );
            }
            objectId = new ObjectId(((Element)children.get(0)).getTextTrim());
            
            // Parse selection
            children = d.getRootElement().getChildren( "selection", esuNs );
            if( children == null || children.size() < 1 ) {
            	throw new EsuException( "selection not found in response" );
            }
            selection = ((Element)children.get(0)).getTextTrim();
            
            // Parse replicas
            children = d.getRootElement().getChildren( "replicas", esuNs );
            if( children == null || children.size() < 1 ) {
            	throw new EsuException( "replicas not found in response" );
            }
            children = ((Element)children.get(0)).getChildren( "replica", esuNs );
            for( Iterator i = children.iterator(); i.hasNext(); ) {
            	Element replica = (Element)i.next();
            	
            	replicas.add( new ObjectReplica(replica) );
            }
            
            // Parse expiration
            children = d.getRootElement().getChildren( "expiration", esuNs );
            if( children == null || children.size() < 1 ) {
            	throw new EsuException( "expiration not found in response" );
            }
            expiration = new ObjectExpiration( (Element)children.get(0) );
            
            // Parse retention
            children = d.getRootElement().getChildren( "retention", esuNs );
            if( children == null || children.size() < 1 ) {
            	throw new EsuException( "retention not found in response" );
            }
            retention = new ObjectRetention( (Element)children.get(0) );
            
        } catch( JDOMException e ) {
        	throw new EsuException( "Error parsing object info", e );
        } catch( IOException e ) {
        	throw new EsuException( "Error parsing object info", e );
        }

	}

	/**
	 * @return the rawXml
	 */
	public String getRawXml() {
		return rawXml;
	}

	/**
	 * @param rawXml the rawXml to set
	 */
	public void setRawXml(String rawXml) {
		this.rawXml = rawXml;
	}

	/**
	 * @return the objectId
	 */
	public ObjectId getObjectId() {
		return objectId;
	}

	/**
	 * @param objectId the objectId to set
	 */
	public void setObjectId(ObjectId objectId) {
		this.objectId = objectId;
	}

	/**
	 * @return the selection
	 */
	public String getSelection() {
		return selection;
	}

	/**
	 * @param selection the selection to set
	 */
	public void setSelection(String selection) {
		this.selection = selection;
	}

	/**
	 * @return the replicas
	 */
	public List<ObjectReplica> getReplicas() {
		return replicas;
	}

	/**
	 * @param replicas the replicas to set
	 */
	public void setReplicas(List<ObjectReplica> replicas) {
		this.replicas = replicas;
	}

	/**
	 * @return the retention
	 */
	public ObjectRetention getRetention() {
		return retention;
	}

	/**
	 * @param retention the retention to set
	 */
	public void setRetention(ObjectRetention retention) {
		this.retention = retention;
	}

	/**
	 * @return the expiration
	 */
	public ObjectExpiration getExpiration() {
		return expiration;
	}

	/**
	 * @param expiration the expiration to set
	 */
	public void setExpiration(ObjectExpiration expiration) {
		this.expiration = expiration;
	}
	
}
