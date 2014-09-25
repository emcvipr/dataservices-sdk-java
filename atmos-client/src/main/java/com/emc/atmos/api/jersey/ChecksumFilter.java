package com.emc.atmos.api.jersey;

import com.emc.atmos.api.ChecksumValue;
import com.emc.atmos.api.ChecksumValueImpl;
import com.emc.atmos.api.ChecksummedInputStream;
import com.emc.atmos.api.RestUtil;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.log4j.Logger;

import javax.ws.rs.HttpMethod;
import java.security.NoSuchAlgorithmException;

public class ChecksumFilter extends ClientFilter {
    private static final Logger l4j = Logger.getLogger(ChecksumFilter.class);

    public ClientResponse handle(ClientRequest request) throws ClientHandlerException {
        try {
            ClientResponse response = getNext().handle(request);

            String checksumHeader = response.getHeaders().getFirst(RestUtil.XHEADER_WSCHECKSUM);
            Object rangeHeader = request.getHeaders().getFirst(RestUtil.HEADER_RANGE);

            // only verify checksum if this is a GET complete object request and there is a ws-checksum header in the
            // response
            if (checksumHeader != null && rangeHeader == null
                    && HttpMethod.GET.equals(request.getMethod()) && response.getLength() > 0) {
                l4j.debug("wschecksum detected (" + checksumHeader + "); wrapping entity stream");
                ChecksumValue checksum = new ChecksumValueImpl(checksumHeader);
                response.setEntityInputStream(new ChecksummedInputStream(response.getEntityInputStream(), checksum));
            }

            return response;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
