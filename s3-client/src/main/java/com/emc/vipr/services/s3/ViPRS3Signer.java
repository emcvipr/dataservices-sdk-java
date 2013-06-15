package com.emc.vipr.services.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.S3Signer;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.emc.vipr.services.s3.model.ViPRConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * This class overrides S3Signer.sign and replaces RestUtils.makeS3CanonicalString for the sole purpose of adding
 * ViPR-specific headers and parameters to the canonical string when signing.
 */
public class ViPRS3Signer extends S3Signer {
    private static final Log log = LogFactory.getLog(ViPRS3Signer.class);

    protected String httpVerb, resourcePath;

    public ViPRS3Signer(String httpVerb, String resourcePath) {
        super(httpVerb, resourcePath);
        this.httpVerb = httpVerb;
        this.resourcePath = resourcePath;
    }

    @Override
    public void sign(Request<?> request, AWSCredentials credentials) throws AmazonClientException {
        if (credentials == null || credentials.getAWSSecretKey() == null) {
            log.debug("Canonical string will not be signed, as no AWS Secret Key was provided");
            return;
        }

        AWSCredentials sanitizedCredentials = sanitizeCredentials(credentials);
        if (sanitizedCredentials instanceof AWSSessionCredentials) {
            addSessionCredentials(request, (AWSSessionCredentials) sanitizedCredentials);
        }

        Date date = getSignatureDate(request.getTimeOffset());
        request.addHeader(Headers.DATE, ServiceUtils.formatRfc822Date(date));
        String canonicalString = makeS3CanonicalString(httpVerb, resourcePath, request, null);
        log.debug("Calculated string to sign:\n\"" + canonicalString + "\"");

        String signature = super.signAndBase64Encode(canonicalString, sanitizedCredentials.getAWSSecretKey(), SigningAlgorithm.HmacSHA1);
        request.addHeader("Authorization", "AWS " + sanitizedCredentials.getAWSAccessKeyId() + ":" + signature);
    }

    /**
     * The set of request parameters which must be included in the canonical
     * string to sign.
     */
    protected static final List<String> SIGNED_PARAMETERS = Arrays.asList(
            "acl", "torrent", "logging", "location", "policy", "requestPayment", "versioning",
            "versions", "versionId", "notification", "uploadId", "uploads", "partNumber", "website",
            "delete", "lifecycle", "tagging", "cors", "restore",
            ResponseHeaderOverrides.RESPONSE_HEADER_CACHE_CONTROL,
            ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_DISPOSITION,
            ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_ENCODING,
            ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_LANGUAGE,
            ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_TYPE,
            ResponseHeaderOverrides.RESPONSE_HEADER_EXPIRES,
            ViPRConstants.ACCESS_MODE_PARAMETER,
            ViPRConstants.FILE_ACCESS_PARAMETER
    );

    /**
     * Calculate the canonical string for a REST/HTTP request to S3.
     * <p/>
     * When expires is non-null, it will be used instead of the Date header.
     */
    protected String makeS3CanonicalString(String method, String resource, Request<?> request, String expires) {
        StringBuilder buf = new StringBuilder();
        buf.append(method).append("\n");

        // Add all interesting headers to a list, then sort them.  "Interesting"
        // is defined as Content-MD5, Content-Type, Date, and x-amz- and x-emc-
        Map<String, String> headersMap = request.getHeaders();
        SortedMap<String, String> interestingHeaders = new TreeMap<String, String>();
        if (headersMap != null && headersMap.size() > 0) {
            for (Map.Entry<String, String> entry : headersMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (key == null) continue;
                String lk = key.toLowerCase(Locale.getDefault());

                // Ignore any headers that are not particularly interesting.
                if (lk.equals("content-type") || lk.equals("content-md5") || lk.equals("date") ||
                        lk.startsWith(Headers.AMAZON_PREFIX)/* || lk.startsWith(ViPRConstants.EMC_PREFIX)*/) {
                    interestingHeaders.put(lk, value);
                }
            }
        }

        // Remove default date timestamp if "x-amz-date" is set.
        if (interestingHeaders.containsKey(Headers.S3_ALTERNATE_DATE)) {
            interestingHeaders.put("date", "");
        }

        // Use the expires value as the timestamp if it is available. This trumps both the default
        // "date" timestamp, and the "x-amz-date" header.
        if (expires != null) {
            interestingHeaders.put("date", expires);
        }

        // These headers require that we still put a new line in after them,
        // even if they don't exist.
        if (!interestingHeaders.containsKey("content-type")) {
            interestingHeaders.put("content-type", "");
        }
        if (!interestingHeaders.containsKey("content-md5")) {
            interestingHeaders.put("content-md5", "");
        }

        // Any parameters that are prefixed with "x-amz-" need to be included
        // in the headers section of the canonical string to sign
        for (Map.Entry<String, String> parameter : request.getParameters().entrySet()) {
            if (parameter.getKey().startsWith("x-amz-")) {
                interestingHeaders.put(parameter.getKey(), parameter.getValue());
            }
        }

        // Add all the interesting headers (i.e.: all that startwith x-amz- ;-))
        for (Map.Entry<String, String> entry : interestingHeaders.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.startsWith(Headers.AMAZON_PREFIX)) {
                buf.append(key).append(':').append(value);
            } else {
                buf.append(value);
            }
            buf.append("\n");
        }

        // Add all the interesting parameters
        buf.append(resource);
        String[] parameterNames = request.getParameters().keySet().toArray(
                new String[request.getParameters().size()]);
        Arrays.sort(parameterNames);
        char separator = '?';
        for (String parameterName : parameterNames) {
            // Skip any parameters that aren't part of the canonical signed string
            if (!SIGNED_PARAMETERS.contains(parameterName)) continue;

            buf.append(separator);
            buf.append(parameterName);
            String parameterValue = request.getParameters().get(parameterName);
            if (parameterValue != null) {
                buf.append("=").append(parameterValue);
            }

            separator = '&';
        }

        return buf.toString();
    }
}
