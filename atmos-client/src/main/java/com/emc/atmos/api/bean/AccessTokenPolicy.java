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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.Date;
import java.util.List;

@XmlRootElement( namespace = "", name = "policy" )
@XmlType( propOrder = {"expiration", "maxUploads", "maxDownloads", "source", "contentLengthRange", "formFieldList"} )
public class AccessTokenPolicy {
    private Date expiration;
    private Source source;
    private int maxUploads;
    private int maxDownloads;
    private ContentLengthRange contentLengthRange;
    private List<FormField> formFieldList;

    @XmlElement( namespace = "" )
    public Date getExpiration() {
        return expiration;
    }

    @XmlElement( namespace = "" )
    public Source getSource() {
        return source;
    }

    @XmlElement( namespace = "", name = "max-uploads" )
    public int getMaxUploads() {
        return maxUploads;
    }

    @XmlElement( namespace = "", name = "max-downloads" )
    public int getMaxDownloads() {
        return maxDownloads;
    }

    @XmlElement( namespace = "", name = "content-length-range" )
    public ContentLengthRange getContentLengthRange() {
        return contentLengthRange;
    }

    @XmlElement( namespace = "", name = "form-field" )
    public List<FormField> getFormFieldList() {
        return formFieldList;
    }

    public void setExpiration( Date expiration ) {
        // we will not send milliseconds, so for comparison accuracy, remove milliseconds here
        long millis = expiration.getTime() % 1000;
        this.expiration = new Date( expiration.getTime() - millis );
    }

    public void setSource( Source source ) {
        this.source = source;
    }

    public void setMaxUploads( int maxUploads ) {
        this.maxUploads = maxUploads;
    }

    public void setMaxDownloads( int maxDownloads ) {
        this.maxDownloads = maxDownloads;
    }

    public void setContentLengthRange( ContentLengthRange contentLengthRange ) {
        this.contentLengthRange = contentLengthRange;
    }

    public void setFormFieldList( List<FormField> formFieldList ) {
        this.formFieldList = formFieldList;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( !(o instanceof AccessTokenPolicy) ) return false;

        AccessTokenPolicy that = (AccessTokenPolicy) o;

        if ( maxDownloads != that.maxDownloads ) return false;
        if ( maxUploads != that.maxUploads ) return false;
        if ( contentLengthRange != null
             ? !contentLengthRange.equals( that.contentLengthRange )
             : that.contentLengthRange != null ) return false;
        if ( expiration != null ? !expiration.equals( that.expiration ) : that.expiration != null ) return false;
        if ( formFieldList != null ? !formFieldList.equals( that.formFieldList ) : that.formFieldList != null )
            return false;
        if ( source != null ? !source.equals( that.source ) : that.source != null ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = expiration != null ? expiration.hashCode() : 0;
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + maxUploads;
        result = 31 * result + maxDownloads;
        result = 31 * result + (contentLengthRange != null ? contentLengthRange.hashCode() : 0);
        result = 31 * result + (formFieldList != null ? formFieldList.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AccessTokenPolicy{" +
               "expiration=" + expiration +
               ", source=" + source +
               ", maxUploads=" + maxUploads +
               ", maxDownloads=" + maxDownloads +
               ", contentLengthRange=" + contentLengthRange +
               ", formFieldList=" + formFieldList +
               '}';
    }

    public static class Source {
        private List<String> allowList;
        private List<String> denyList;

        @XmlElement( namespace = "", name = "allow" )
        public List<String> getAllowList() {
            return allowList;
        }

        @XmlElement( namespace = "", name = "disallow" )
        public List<String> getDenyList() {
            return denyList;
        }

        public void setAllowList( List<String> allowList ) {
            this.allowList = allowList;
        }

        public void setDenyList( List<String> denyList ) {
            this.denyList = denyList;
        }

        @Override
        public boolean equals( Object o ) {
            if ( this == o ) return true;
            if ( o == null || getClass() != o.getClass() ) return false;

            Source source = (Source) o;

            if ( allowList != null ? !allowList.equals( source.allowList ) : source.allowList != null ) return false;
            if ( denyList != null ? !denyList.equals( source.denyList ) : source.denyList != null ) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = allowList != null ? allowList.hashCode() : 0;
            result = 31 * result + (denyList != null ? denyList.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Source{" +
                   "allowList=" + allowList +
                   ", denyList=" + denyList +
                   '}';
        }
    }

    public static class ContentLengthRange {
        private int from;
        private int to;

        @XmlAttribute
        public int getFrom() {
            return from;
        }

        @XmlAttribute
        public int getTo() {
            return to;
        }

        public void setFrom( int from ) {
            this.from = from;
        }

        public void setTo( int to ) {
            this.to = to;
        }

        @Override
        public boolean equals( Object o ) {
            if ( this == o ) return true;
            if ( o == null || getClass() != o.getClass() ) return false;

            ContentLengthRange that = (ContentLengthRange) o;

            if ( from != that.from ) return false;
            if ( to != that.to ) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = from;
            result = 31 * result + to;
            return result;
        }

        @Override
        public String toString() {
            return "ContentLengthRange{" +
                   "from=" + from +
                   ", to=" + to +
                   '}';
        }
    }

    public static class FormField {
        private String name;
        private boolean optional;
        private String eq;
        private String startsWith;
        private String endsWith;
        private String contains;
        private String matches;

        @XmlAttribute
        public String getName() {
            return name;
        }

        @XmlAttribute
        public boolean isOptional() {
            return optional;
        }

        @XmlElement( namespace = "" )
        public String getEq() {
            return eq;
        }

        @XmlElement( namespace = "", name = "starts-with" )
        public String getStartsWith() {
            return startsWith;
        }

        @XmlElement( namespace = "", name = "ends-with" )
        public String getEndsWith() {
            return endsWith;
        }

        @XmlElement( namespace = "" )
        public String getContains() {
            return contains;
        }

        @XmlElement( namespace = "" )
        public String getMatches() {
            return matches;
        }

        public void setName( String name ) {
            this.name = name;
        }

        public void setOptional( boolean optional ) {
            this.optional = optional;
        }

        public void setEq( String eq ) {
            this.eq = eq;
        }

        public void setStartsWith( String startsWith ) {
            this.startsWith = startsWith;
        }

        public void setEndsWith( String endsWith ) {
            this.endsWith = endsWith;
        }

        public void setContains( String contains ) {
            this.contains = contains;
        }

        public void setMatches( String matches ) {
            this.matches = matches;
        }

        @Override
        public boolean equals( Object o ) {
            if ( this == o ) return true;
            if ( o == null || getClass() != o.getClass() ) return false;

            FormField formField = (FormField) o;

            if ( optional != formField.optional ) return false;
            if ( contains != null ? !contains.equals( formField.contains ) : formField.contains != null ) return false;
            if ( endsWith != null ? !endsWith.equals( formField.endsWith ) : formField.endsWith != null ) return false;
            if ( eq != null ? !eq.equals( formField.eq ) : formField.eq != null ) return false;
            if ( matches != null ? !matches.equals( formField.matches ) : formField.matches != null ) return false;
            if ( !name.equals( formField.name ) ) return false;
            if ( startsWith != null ? !startsWith.equals( formField.startsWith ) : formField.startsWith != null )
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + (optional ? 1 : 0);
            result = 31 * result + (eq != null ? eq.hashCode() : 0);
            result = 31 * result + (startsWith != null ? startsWith.hashCode() : 0);
            result = 31 * result + (endsWith != null ? endsWith.hashCode() : 0);
            result = 31 * result + (contains != null ? contains.hashCode() : 0);
            result = 31 * result + (matches != null ? matches.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "FormField{" +
                   "name='" + name + '\'' +
                   ", optional=" + optional +
                   ", eq='" + eq + '\'' +
                   ", startsWith='" + startsWith + '\'' +
                   ", endsWith='" + endsWith + '\'' +
                   ", contains='" + contains + '\'' +
                   ", matches='" + matches + '\'' +
                   '}';
        }
    }
}
