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

package com.emc.acdp.api;

import com.emc.cdp.services.rest.model.*;

import java.util.Date;
import java.util.List;

/**
 * Interface for Atmos Cloud Delivery Platform API
 *
 * @author cwikj
 */
public interface AcdpAdminApi {
    String createAccount( Account acct );

    void deleteAccount( String accountId );

    AccountList listAccounts( boolean includeSubscription );

    AccountList listAccounts( boolean includeSubscription, int start, int count );

    String createAccountInvitation( String accountId, String email, String accountRole );

    String createSubscription( String accountId, String serviceId );

    void provisionSubscription( String accountId, String subscriptionId, boolean sendEmail );

    Account getIdentityAccount( String identity, boolean includeSubscription );

    SubscriptionList getAccountSubscriptions( String accountId );

    Subtenant getSubtenant( String accountId, String subscriptionId );

    void adminAccountEvent( String accountId, LifecycleEventType adminSuspend );

    void deleteIdentity( String identityId );

    void addAccountAssignee( String accountId,
                             String identityId,
                             String password,
                             String firstName,
                             String lastName,
                             String email,
                             String role );

    AssigneeList listAccountAssignees( String accountId, boolean includeProfile );

    Assignee getAccountAssignee( String accountId, String identityId, boolean includeProfile );

    void editAccountAssignee( String accountId, String identityId, String newRole );

    void RemoveAccountAssignee( String accountId, String identityId );

    void updateIdentityProfile( String identityId, Profile p );

    IdentityList listIdentities( boolean listAllAccounts, boolean includeProfile );

    IdentityList listIdentities( boolean listAllAccounts, boolean includeProfile, int start, int count );

    Identity getIdentity( String identityId );

    Account getAccount( String accountId );

    MeteringUsageList getSubscriptionUsage( String accountId,
                                            String subscriptionId,
                                            Date startDate,
                                            Date endDate,
                                            List<String> resources,
                                            String category );

    MeteringUsageList getSubscriptionUsage( String accountId,
                                            String subscriptionId,
                                            Date startDate,
                                            Date endDate,
                                            List<String> resources,
                                            String category,
                                            int start,
                                            int count );

    void deleteSubscription( String accountId, String subscriptionId );

    void unassignAccountIdentity( String accountId, String identityId );

    TokenGroupList listTokenGroups( String accountId, String subscriptionId );

    TokenGroupList listTokenGroups( String accountId, String subscriptionId, int start, int count );

    MeteringUsageList getTokenGroupUsage( String accountId,
                                          String subscriptionId,
                                          String tokenGroupId,
                                          Date startDate,
                                          Date endDate,
                                          List<String> resources,
                                          String category );

    MeteringUsageList getTokenGroupUsage( String accountId,
                                          String subscriptionId,
                                          String tokenGroupId,
                                          Date startDate,
                                          Date endDate,
                                          List<String> resources,
                                          String category,
                                          int start,
                                          int count );

    TokenList listTokens( String accountId, String subscriptionId, String tokenGroupId );

    TokenList listTokens( String accountId, String subscriptionId, String tokenGroupId, int start, int count );

    Token getTokenInformation( String accountId,
                               String subscriptionId,
                               String tokenGroupId,
                               String tokenId,
                               boolean showFullInfo );
}
