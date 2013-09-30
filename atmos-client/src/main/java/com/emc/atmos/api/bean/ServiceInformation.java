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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains information from the GetServiceInformation call
 *
 * @author jason
 */
@XmlRootElement( name = "Service" )
public class ServiceInformation {
    private Version version;
    private Set<Feature> features;

    public ServiceInformation() {
        features = new HashSet<Feature>();
    }

    /**
     * @return the atmosVersion
     */
    @XmlTransient
    public String getAtmosVersion() {
        if ( version == null ) return null;
        return version.getAtmos();
    }

    @XmlElement( name = "Version" )
    public Version getVersion() {
        return version;
    }

    public void setVersion( Version version ) {
        this.version = version;
    }

    /**
     * Adds a feature to the list of supported features.
     */
    public void addFeature( Feature feature ) {
        features.add( feature );
    }

    public void addFeatureFromHeaderName( String headerName ) {
        features.add( Feature.fromHeaderName( headerName ) );
    }

    /**
     * Checks to see if a feature is supported.
     */
    public boolean hasFeature( Feature feature ) {
        return features.contains( feature );
    }

    /**
     * Gets the features advertised by the service
     */
    @XmlTransient
    public Set<Feature> getFeatures() {
        return Collections.unmodifiableSet( features );
    }

    private static class Version {
        private String atmos;

        @XmlElement( name = "Atmos" )
        public String getAtmos() {
            return atmos;
        }

        public void setAtmos( String atmos ) {
            this.atmos = atmos;
        }
    }

    @XmlTransient
    public static enum Feature {
        Object( "object" ),
        Namespace( "namespace" ),
        Utf8( "utf-8" ),
        BrowserCompat( "browser-compat" ),
        KeyValue( "key-value" ),
        Hardlink( "hardlink" ),
        Query( "query" ),
        Versioning( "versioning" );

        public static Feature fromHeaderName( String headerName ) {
            for ( Feature feature : values() )
                if ( feature.getHeaderName().equals( headerName ) ) return feature;
            return null;
        }

        private String headerName;

        private Feature( String headerName ) {
            this.headerName = headerName;
        }

        public String getHeaderName() {
            return headerName;
        }
    }
}
