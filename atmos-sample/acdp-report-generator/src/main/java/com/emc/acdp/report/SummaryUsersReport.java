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

import java.util.*;

public class SummaryUsersReport extends AcdpReport {
    private static final Logger log = Logger.getLogger( SummaryUsersReport.class );

    private static final String LAST_NAME = "lastName";
    private static final String FIRST_NAME = "firstName";
    private static final String DATE_CREATED = "dateCreated";
    private static final String USER_ID = "userId";
    private static final String ACCOUNT_ROLE = "accountRole";
    private static final String COMPANY = "company";
    private static final String COMPANY_SIZE = "companySize";
    private static final String COMPANY_URL = "companyUrl";
    private static final String INDUSTRY_CODE = "industryCode";
    private static final String ADDRESS_1 = "address1";
    private static final String ADDRESS_2 = "address2";
    private static final String CITY = "city";
    private static final String STATE = "state";
    private static final String COUNTY = "county";
    private static final String COUNTRY_CODE = "countryCode";
    private static final String POSTCODE = "postcode";
    private static final String EMAIL = "email";
    private static final String IM = "im";
    private static final String WORK_PHONE = "workPhone";
    private static final String MOBILE_PHONE = "mobilePhone";
    private static final String HOME_PHONE = "homePhone";
    private static final String FAX = "fax";
    private static final String PAGER = "pager";
    private static final String PROFILE_ATTRIBUTES = "profileAttributes";
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

    @Override
    protected List<ReportRow> getSeedData() {
        log.debug( "Collecting seed data" );

        List<ReportRow> rows = new ArrayList<ReportRow>();

        // TODO: support more than 1M identities?
        IdentityList identityList = adminApi.listIdentities( true, true, 1, 1000000 );

        int rowIndex = 0;
        ReportRow row;
        Profile profile;
        for ( Identity identity : identityList.getIdentities() ) {
            row = new ReportRow( rowIndex++ );

            profile = identity.getProfile();
            if ( profile == null ) profile = new Profile();

            row.put( LAST_NAME, profile.getLastName() );
            row.put( FIRST_NAME, profile.getFirstName() );
            row.put( DATE_CREATED, identity.getSignUpTime() );
            row.put( USER_ID, identity.getId() );
            row.put( ACCOUNT_ROLE, identity.getRole() );
            row.put( COMPANY, profile.getCompanyName() );
            row.put( COMPANY_SIZE, profile.getCompanySize() );
            row.put( COMPANY_URL, profile.getCompanyURL() );
            row.put( INDUSTRY_CODE, profile.getIndustryCode() );
            row.put( ADDRESS_1, profile.getAddress1() );
            row.put( ADDRESS_2, profile.getAddress2() );
            row.put( CITY, profile.getCity() );
            row.put( STATE, profile.getStateCode() );
            row.put( COUNTY, profile.getCounty() );
            row.put( COUNTRY_CODE, profile.getCountryCode() );
            row.put( POSTCODE, profile.getPostalCode() );
            row.put( EMAIL, profile.getEmail() );
            row.put( IM, profile.getIm() );
            row.put( WORK_PHONE, profile.getWorkPhone() );
            row.put( MOBILE_PHONE, profile.getMobilePhone() );
            row.put( HOME_PHONE, profile.getHomePhone() );
            row.put( FAX, profile.getFax() );
            row.put( PAGER, profile.getPager() );

            rows.add( row );
        }

        // sort
        log.debug( "Sorting seeded rows" );
        Collections.sort( rows, new Comparator<ReportRow>() {
            @Override
            public int compare( ReportRow a, ReportRow b ) {
                return concatNullsLast( a.get( LAST_NAME ),
                                        a.get( FIRST_NAME ) ).compareToIgnoreCase( concatNullsLast( b.get( LAST_NAME ),
                                                                                                    b.get( FIRST_NAME ) ) );
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

        String identityId = (String) seededRow.get( USER_ID );

        LogMF.debug( log, "++Filling row index {0} (identityId: {1})",
                     seededRow.getIndex(), identityId );
        Account account = null;
        try {
            account = adminApi.getIdentityAccount( identityId, true );
            LogMF.debug( log, "||Row Index {0}||: Found account ({1})",
                         seededRow.getIndex(), account.getId() );
        } catch ( AcdpException e ) {
            if ( !"AccountNotExistForIdentity".equals( e.getAcdpCode() ) ) throw e;
            log.info( "||Row Index " + seededRow.getIndex() + "||: Account not found" );
        }

        if ( account != null ) {
            Subscription subscription;
            if ( account.getSubscriptions() != null && !account.getSubscriptions().isEmpty() ) {
                subscription = account.getSubscriptions().iterator().next();
                LogMF.debug( log, "||Row Index {0}||: Found subscription ({1})",
                             seededRow.getIndex(), subscription.getId() );
            } else {
                subscription = new Subscription();
                log.info( "||Row Index " + seededRow.getIndex() + "||: Subscription not found" );
            }

            StringBuilder sb;

            seededRow.put( ACCOUNT_ID, account.getId() );
            seededRow.put( ACCOUNT_TYPE, account.getType() );
            seededRow.put( ACCOUNT_STATE, account.getState() );
            seededRow.put( ACCOUNT_NAME, account.getName() );
            seededRow.put( ACCOUNT_DESCRIPTION, account.getDescription() );
            if ( account.getAttributes() != null ) {
                sb = new StringBuilder();
                for ( Attribute attribute : account.getAttributes() ) {
                    if ( sb.length() > 0 ) sb.append( ", " );
                    sb.append( attribute.getName() ).append( ": " ).append( attribute.getValue() );
                }
                seededRow.put( ACCOUNT_ATTRIBUTES, sb.toString() );
            }
            seededRow.put( SUBSCRIPTION_ID, subscription.getId() );
            seededRow.put( START_DATE, subscription.getEffectiveDate() );
            seededRow.put( SUBSCRIPTION_STATE,
                           (subscription.getState() == null) ? "" : subscription.getState().value() );
            if ( subscription.getAttributes() != null ) {
                sb = new StringBuilder();
                for ( Attribute attribute : subscription.getAttributes() ) {
                    if ( sb.length() > 0 ) sb.append( ", " );
                    sb.append( attribute.getName() ).append( ": " ).append( attribute.getValue() );
                }
                seededRow.put( SUBSCRIPTION_ATTRIBUTES, sb.toString() );
            }
        }

        rows.add( seededRow );

        LogMF.debug( log, "--Filled row index {0}: {1} sub-rows (identityId: {2})",
                     seededRow.getIndex(), rows.size(), identityId );
        return rows;
    }

    @Override
    protected String[] getColumnNames() {
        return new String[]{
                LAST_NAME,
                FIRST_NAME,
                DATE_CREATED,
                USER_ID,
                ACCOUNT_ROLE,
                COMPANY,
                COMPANY_SIZE,
                COMPANY_URL,
                INDUSTRY_CODE,
                ADDRESS_1,
                ADDRESS_2,
                CITY,
                STATE,
                COUNTY,
                COUNTRY_CODE,
                POSTCODE,
                EMAIL,
                IM,
                WORK_PHONE,
                MOBILE_PHONE,
                HOME_PHONE,
                FAX,
                PAGER,
                PROFILE_ATTRIBUTES,
                ACCOUNT_ID,
                ACCOUNT_TYPE,
                ACCOUNT_STATE,
                ACCOUNT_NAME,
                ACCOUNT_DESCRIPTION,
                ACCOUNT_ATTRIBUTES,
                SUBSCRIPTION_ID,
                START_DATE,
                SUBSCRIPTION_STATE,
                SUBSCRIPTION_ATTRIBUTES
        };
    }

    @Override
    protected Map<String, String> getHeaderLabels() {
        Map<String, String> labelMap = new HashMap<String, String>();
        labelMap.put( LAST_NAME, "Last Name" );
        labelMap.put( FIRST_NAME, "First Name" );
        labelMap.put( DATE_CREATED, "Date Created" );
        labelMap.put( USER_ID, "User ID" );
        labelMap.put( ACCOUNT_ROLE, "Account Role" );
        labelMap.put( COMPANY, "Company" );
        labelMap.put( COMPANY_SIZE, "Company Size" );
        labelMap.put( COMPANY_URL, "Company URL" );
        labelMap.put( INDUSTRY_CODE, "Industry Code" );
        labelMap.put( ADDRESS_1, "Address 1" );
        labelMap.put( ADDRESS_2, "Address 2" );
        labelMap.put( CITY, "City" );
        labelMap.put( STATE, "State" );
        labelMap.put( COUNTY, "County" );
        labelMap.put( COUNTRY_CODE, "Country Code" );
        labelMap.put( POSTCODE, "Postcode" );
        labelMap.put( EMAIL, "Email" );
        labelMap.put( IM, "IM" );
        labelMap.put( WORK_PHONE, "Work Phone" );
        labelMap.put( MOBILE_PHONE, "Mobile Phone" );
        labelMap.put( HOME_PHONE, "Home Phone" );
        labelMap.put( FAX, "Fax" );
        labelMap.put( PAGER, "Pager" );
        labelMap.put( PROFILE_ATTRIBUTES, "Profile Attributes" );
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
        return labelMap;
    }
}
