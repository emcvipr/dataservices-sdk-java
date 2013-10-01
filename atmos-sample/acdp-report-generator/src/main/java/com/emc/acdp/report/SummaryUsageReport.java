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
package com.emc.acdp.report;

import com.emc.acdp.AcdpException;
import com.emc.cdp.services.rest.model.*;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.*;

public class SummaryUsageReport extends AcdpReport {
    private static final Logger log = Logger.getLogger( SummaryUsageReport.class );

    private static final String ACCOUNT_ID = "accountId";
    private static final String ACCOUNT_TYPE = "accountType";
    private static final String ACCOUNT_STATE = "accountState";
    private static final String ACCOUNT_NAME = "accountName";
    private static final String ACCOUNT_DESCRIPTION = "accountDescription";
    private static final String ACCOUNT_ATTRIBUTES = "accountAttributes";
    private static final String SUBSCRIPTION_ID = "subscriptionId";
    private static final String START_DATE = "startDate";
    private static final String SUBSCRIPTION_STATE = "subscriptionState";
    private static final String SUBSCRIPTION_ATTRIBUTES = "subscriptionAttributes";
    private static final String SUBTENANT_ID = "subtenantId";
    private static final String TOKEN_GROUP_ID = "tokenGroupId";
    private static final String TOKEN_ID = "tokenId";
    private static final String POLICY_OR_ACCESS_METHOD = "policyOrAccessMethod";
    private static final String RESOURCE_TYPE = "unitType"; // i.e. Disk Usage, Bandwidth In, Bandwidth Out
    private static final String USAGE_VALUE = "total";

    @Override
    protected List<ReportRow> getSeedData() {
        log.debug( "Collecting seed data" );

        List<ReportRow> rows = new ArrayList<ReportRow>();

        AccountList accountList = adminApi.listAccounts( true, 1, 1000000 ); // TODO: support more than 1M accounts?

        int rowIndex = 0;
        for ( Account account : accountList.getAccounts() ) {
            if ( account.getSubscriptions() == null || account.getSubscriptions().isEmpty() )
                continue;

            ReportRow row = new ReportRow( rowIndex++ );
            Subscription subscription = account.getSubscriptions().iterator().next();

            row.put( ACCOUNT_ID, account.getId() );
            row.put( ACCOUNT_TYPE, account.getType() );
            row.put( ACCOUNT_STATE, account.getState() );
            row.put( ACCOUNT_NAME, account.getName() );
            row.put( ACCOUNT_DESCRIPTION, account.getDescription() );
            if ( account.getAttributes() != null ) {
                StringBuilder sb = new StringBuilder();
                for ( Attribute attribute : account.getAttributes() ) {
                    if ( sb.length() > 0 ) sb.append( ", " );
                    sb.append( attribute.getName() ).append( ": " ).append( attribute.getValue() );
                }
                row.put( ACCOUNT_ATTRIBUTES, sb.toString() );
            }
            row.put( SUBSCRIPTION_ID, subscription.getId() );
            row.put( START_DATE, subscription.getEffectiveDate() );
            row.put( SUBSCRIPTION_STATE, subscription.getState().value() );
            if ( subscription.getAttributes() != null ) {
                StringBuilder sb = new StringBuilder();
                for ( Attribute attribute : subscription.getAttributes() ) {
                    if ( sb.length() > 0 ) sb.append( ", " );
                    sb.append( attribute.getName() ).append( ": " ).append( attribute.getValue() );
                }
                row.put( SUBSCRIPTION_ATTRIBUTES, sb.toString() );
            }
            rows.add( row );
        }

        // sort
        log.debug( "Sorting seeded rows" );
        Collections.sort( rows, new Comparator<ReportRow>() {
            @Override
            public int compare( ReportRow a, ReportRow b ) {
                return concatNullsLast( a.get( ACCOUNT_STATE ),
                                        a.get( SUBSCRIPTION_STATE ),
                                        a.get( ACCOUNT_ID ) ).compareTo( concatNullsLast( b.get( ACCOUNT_STATE ),
                                                                                          b.get( SUBSCRIPTION_STATE ),
                                                                                          b.get( ACCOUNT_ID ) ) );
            }
        } );

        // assign indexes based on sort results (determines rendering order)
        int index = 0;
        for ( ReportRow r : rows ) {
            r.setIndex( index++ );
        }

        log.debug( "Collected " + rows.size() + " seeded rows" );
        return rows;
    }

    @Override
    protected List<ReportRow> fillRow( ReportRow seededRow ) {
        List<ReportRow> rows = new ArrayList<ReportRow>();

        Calendar now = Calendar.getInstance();
        Calendar weekAgo = Calendar.getInstance();
        weekAgo.add( Calendar.DAY_OF_YEAR, -7 );
        Calendar hourAgo = Calendar.getInstance();
        hourAgo.add( Calendar.HOUR, -1 );

        String accountId = (String) seededRow.get( ACCOUNT_ID );
        String subscriptionId = (String) seededRow.get( SUBSCRIPTION_ID );

        LogMF.debug( log, "++Filling row index {0} (accountId: {1}, subscriptionId {2})",
                     seededRow.getIndex(), accountId, subscriptionId );

        // get the subtenant id (the only way to get it is to make a separate call per subscription)
        Subtenant subtenant;
        try {
            subtenant = adminApi.getSubtenant( accountId, subscriptionId );
            LogMF.debug( log, "||Row Index {0}||: Found subtenant ({1})",
                         seededRow.getIndex(), subtenant.getId() );
        } catch ( AcdpException e ) {
            if ( !"SubtenantNotExist".equals( e.getAcdpCode() ) ) throw e;
            subtenant = new Subtenant();
            log.info( "||Row Index " + seededRow.getIndex() + "||: Subtenant not found" );
        }

        // collect bandwidth usage (total over the last week)
        MeteringUsageList bandwidthUsageList = adminApi.getSubscriptionUsage( accountId,
                                                                              subscriptionId,
                                                                              weekAgo.getTime(),
                                                                              now.getTime(),
                                                                              Arrays.asList( "BandwidthIn/*",
                                                                                             "BandwidthOut/*" ),
                                                                              "bytoken" );
        for ( MeteringUsage meteringUsage : bandwidthUsageList.getMeteringUsages() ) {
            String groupId = meteringUsage.getTokenGroupId();
            String tokenId = meteringUsage.getTokenId();
            LogMF.debug( log, "||Row Index {0}||: Received metrics for (tokenGroupId: {1}, tokenId: {2})",
                         seededRow.getIndex(), groupId, tokenId );
            boolean bwi = false, bwo = false;
            for ( Usage usage : meteringUsage.getUsages() ) {
                if ( "BandwidthIn".equals( usage.getResource() ) ) bwi = true;
                else if ( "BandwidthOut".equals( usage.getResource() ) ) bwo = true;
                ReportRow row = seededRow.clone();
                row.put( SUBTENANT_ID, subtenant.getId() );
                row.put( TOKEN_GROUP_ID, groupId );
                row.put( TOKEN_ID, tokenId );
                row.put( POLICY_OR_ACCESS_METHOD, usage.getResourceTag() );
                row.put( RESOURCE_TYPE, usage.getResource() );
                row.put( USAGE_VALUE, usage.getQuantum() );
                rows.add( row );
            }
            if ( !bwi ) {
                log.debug( "||Row Index " + seededRow.getIndex() + "||: No BandwidthIn metrics; assuming 0" );
                ReportRow row = seededRow.clone();
                row.put( SUBTENANT_ID, subtenant.getId() );
                row.put( TOKEN_GROUP_ID, groupId );
                row.put( TOKEN_ID, tokenId );
                row.put( POLICY_OR_ACCESS_METHOD, "All" );
                row.put( RESOURCE_TYPE, "BandwidthIn" );
                row.put( USAGE_VALUE, new BigDecimal( 0 ) );
                rows.add( row );
            }
            if ( !bwo ) {
                log.debug( "||Row Index " + seededRow.getIndex() + "||: No BandwidthOut metrics; assuming 0" );
                ReportRow row = seededRow.clone();
                row.put( SUBTENANT_ID, subtenant.getId() );
                row.put( TOKEN_GROUP_ID, groupId );
                row.put( TOKEN_ID, tokenId );
                row.put( POLICY_OR_ACCESS_METHOD, "All" );
                row.put( RESOURCE_TYPE, "BandwidthOut" );
                row.put( USAGE_VALUE, new BigDecimal( 0 ) );
                rows.add( row );
            }
        }

        // collect disk usage (as of an hour ago)
        MeteringUsageList diskUsageList = adminApi.getSubscriptionUsage( accountId,
                                                                         subscriptionId,
                                                                         hourAgo.getTime(),
                                                                         now.getTime(),
                                                                         Arrays.asList( "DiskUsage/*" ),
                                                                         "bytoken" );
        for ( MeteringUsage meteringUsage : diskUsageList.getMeteringUsages() ) {
            String groupId = meteringUsage.getTokenGroupId();
            String tokenId = meteringUsage.getTokenId();
            for ( Usage usage : meteringUsage.getUsages() ) {
                ReportRow row = seededRow.clone();
                row.put( SUBTENANT_ID, subtenant.getId() );
                row.put( TOKEN_GROUP_ID, groupId );
                row.put( TOKEN_ID, tokenId );
                row.put( POLICY_OR_ACCESS_METHOD, usage.getResourceTag() );
                row.put( RESOURCE_TYPE, usage.getResource() );
                row.put( USAGE_VALUE, usage.getQuantum() );
                rows.add( row );
            }
            if ( meteringUsage.getUsages().isEmpty() ) {
                log.debug( "||Row Index " + seededRow.getIndex() + "||: No DiskUsage metrics; assuming 0" );
                ReportRow row = seededRow.clone();
                row.put( SUBTENANT_ID, subtenant.getId() );
                row.put( TOKEN_GROUP_ID, groupId );
                row.put( TOKEN_ID, tokenId );
                row.put( POLICY_OR_ACCESS_METHOD, "All" );
                row.put( RESOURCE_TYPE, "DiskUsage" );
                row.put( USAGE_VALUE, new BigDecimal( 0 ) );
                rows.add( row );
            }
        }

        if ( rows.isEmpty() ) {
            log.info( "||Row Index " + seededRow.getIndex() + "||: No metrics found" );
            ReportRow row = seededRow.clone();
            row.put( SUBTENANT_ID, subtenant.getId() );
            rows.add( row );
        }

        // sort
        log.debug( "||Row Index " + seededRow.getIndex() + "||: Sorting sub-rows" );
        Collections.sort( rows, new Comparator<ReportRow>() {
            @Override
            public int compare( ReportRow a, ReportRow b ) {
                return concatNullsLast( a.get( TOKEN_GROUP_ID ),
                                        a.get( TOKEN_ID ),
                                        a.get( RESOURCE_TYPE ),
                                        a.get( POLICY_OR_ACCESS_METHOD ) )
                        .compareTo( concatNullsLast( b.get( TOKEN_GROUP_ID ),
                                                     b.get( TOKEN_ID ),
                                                     b.get( RESOURCE_TYPE ),
                                                     b.get( POLICY_OR_ACCESS_METHOD ) ) );
            }
        } );

        // assign subIndexes based on sort results (determines rendering order)
        int subIndex = 0;
        for ( ReportRow row : rows ) {
            row.setSubIndex( subIndex++ );
        }

        LogMF.debug( log, "--Filled row index {0}: {1} sub-rows (accountId: {2}, subscriptionId {3})",
                     seededRow.getIndex(), rows.size(), accountId, subscriptionId );
        return rows;
    }

    @Override
    protected String[] getColumnNames() {
        return new String[]{
                ACCOUNT_ID,
                ACCOUNT_TYPE,
                ACCOUNT_STATE,
                ACCOUNT_NAME,
                ACCOUNT_DESCRIPTION,
                ACCOUNT_ATTRIBUTES,
                SUBSCRIPTION_ID,
                START_DATE,
                SUBSCRIPTION_STATE,
                SUBSCRIPTION_ATTRIBUTES,
                SUBTENANT_ID,
                TOKEN_GROUP_ID,
                TOKEN_ID,
                POLICY_OR_ACCESS_METHOD,
                RESOURCE_TYPE, // i.e. DiskUsage, BandwidthIn, BandwidthOut
                USAGE_VALUE
        };
    }

    @Override
    @SuppressWarnings( "unchecked" )
    protected List<Map<String, String>> getReportNotes() {
        Map<String, String> notesMap = new HashMap<String, String>();
        notesMap.put( ACCOUNT_ID, "NOTE: bandwidth values are aggregated over the past week" );
        return Arrays.asList( notesMap );
    }

    @Override
    protected Map<String, String> getHeaderLabels() {
        Map<String, String> labelMap = new HashMap<String, String>();
        labelMap.put( ACCOUNT_ID, "Account ID" );
        labelMap.put( ACCOUNT_TYPE, "Account Type" );
        labelMap.put( ACCOUNT_STATE, "Account State" );
        labelMap.put( ACCOUNT_NAME, "Account Name" );
        labelMap.put( ACCOUNT_DESCRIPTION, "Account Description" );
        labelMap.put( ACCOUNT_ATTRIBUTES, "Account Attributes" );
        labelMap.put( SUBSCRIPTION_ID, "Subscription ID" );
        labelMap.put( START_DATE, "Start Date" );
        labelMap.put( SUBSCRIPTION_STATE, "Subscription Status" );
        labelMap.put( SUBSCRIPTION_ATTRIBUTES, "Subscription Attributes" );
        labelMap.put( SUBTENANT_ID, "Subtenant ID" );
        labelMap.put( TOKEN_GROUP_ID, "Token Group ID" );
        labelMap.put( TOKEN_ID, "Token ID" );
        labelMap.put( POLICY_OR_ACCESS_METHOD, "Policy/Access Method" );
        labelMap.put( RESOURCE_TYPE, "Resource Type" );
        labelMap.put( USAGE_VALUE, "Usage Value" );
        return labelMap;
    }
}
