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
package com.emc.atmos.test.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.*;

public class ListBucket {
    public static final int DEFAULT_MAX_KEYS = 10000;

    public static void main(String[] args) {
        ListBucket listBucket = fromCli(args);
        
        if(listBucket.autoIterate) {
            listBucket.testAuto();
        } else {
            listBucket.test();
        }
    }

    public static ListBucket fromCli(String[] args) {
        GnuParser gnuParser = new GnuParser();
        CommandLine line = null;
        try {
            line = gnuParser.parse(getOptions(), args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            help();
            System.exit(2);
        }

        // Special check for help
        if (line.hasOption("h")) {
            help();
            System.exit(0);
        }

        if (line.getArgs().length == 0) {
            help();
            System.exit(2);
        }

        String bucket = line.getArgs()[0];
        ListBucket listBucket = new ListBucket(bucket);

        if (line.hasOption("p")) listBucket.setPrefix(line.getOptionValue("p"));
        if (line.hasOption("m")) listBucket.setMarker(line.getOptionValue("m"));
        if (line.hasOption("d")) listBucket.setDelimiter(line.getOptionValue("d"));
        if (line.hasOption("k")) listBucket.setMaxKeys(Integer.parseInt(line.getOptionValue("k")));
        if (line.hasOption('i')) listBucket.setAutoIterate(true);
        
        return listBucket;
    }

    @SuppressWarnings("static-access")
    protected static Options getOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder.withDescription("S3 bucket prefix").hasArg().withArgName("prefix").create("p"));
        options.addOption(OptionBuilder.withDescription("S3 bucket marker").hasArg().withArgName("marker").create("m"));
        options.addOption(OptionBuilder.withDescription("S3 bucket delimiter").hasArg().withArgName("delimiter").create("d"));
        options.addOption(OptionBuilder.withDescription("S3 bucket max keys").hasArg().withArgName("max-keys").create("k"));
        options.addOption(OptionBuilder.withDescription("Show this help text").create("h"));
        options.addOption(OptionBuilder.withDescription("Automatically iterate through markers").create('i'));
        return options;
    }

    protected static void help() {
        HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp("java -jar ListBucket.jar [options ...] bucket-name\nOptions:", getOptions());
    }

    private String bucket;
    private String prefix;
    private String marker;
    private String delimiter;
    private int maxKeys = DEFAULT_MAX_KEYS;
    private boolean autoIterate;
    private ObjectListing listing;
    private long duration;
    private int resultCount;

    public ListBucket(String bucket) {
        this.bucket = bucket;
    }
    
    public void testAuto() {
        long totalResults = 0;
        long totalTime = 0;
        
        do {
            test();
            totalResults += resultCount;
            totalTime += duration;
            System.out.printf("\n**Total Results So Far: %d\n", totalResults);
            System.out.printf("**Total Time So Far: %d\n", totalTime);
            setMarker(listing.getNextMarker());
        } while(listing.isTruncated());
    }

    public void test() {
        AmazonS3 s3 = ClientFactory.getS3Client();

        long start = System.currentTimeMillis();
        ListObjectsRequest listRequest = new ListObjectsRequest(bucket, prefix, marker, delimiter, maxKeys);
        listing = s3.listObjects(listRequest);
        long end = System.currentTimeMillis();
        duration = end-start;
        resultCount = listing.getObjectSummaries().size();

        System.out.printf("Bucket: %s\t", listing.getBucketName());
        System.out.printf("Prefix: %s\n", listing.getPrefix());
        System.out.printf("Marker: %s\t", listing.getMarker());
        System.out.printf("Delimiter: %s\t", listing.getDelimiter());
        System.out.printf("Max-Keys: %d\n\n", listing.getMaxKeys());
        System.out.printf("Truncated: %b\t", listing.isTruncated());
        System.out.printf("Next Marker: %s\n", listing.getNextMarker());
        System.out.printf("Call Duration: %dms\tResults: %d\n", duration, resultCount);
        System.out.println("----------------------------");

        for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            System.out.printf("%-40s %19d\n", summary.getKey(), summary.getSize());
        }

        System.out.println("----- Common Prefixes ------");
        for (String prefix : listing.getCommonPrefixes()) {
            System.out.println(prefix);
        }
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getMarker() {
        return marker;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public void setMaxKeys(int maxKeys) {
        this.maxKeys = maxKeys;
    }
    
    public void setAutoIterate(boolean auto) {
        this.autoIterate = auto;
    }
    
    public boolean isAutoIterate() {
        return this.autoIterate;
    }

    /**
     * @return the listing
     */
    public ObjectListing getListing() {
        return listing;
    }

    /**
     * @param listing the listing to set
     */
    public void setListing(ObjectListing listing) {
        this.listing = listing;
    }
}
