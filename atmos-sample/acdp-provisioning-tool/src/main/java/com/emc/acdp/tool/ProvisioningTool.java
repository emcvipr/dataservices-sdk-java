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
package com.emc.acdp.tool;

import com.emc.acdp.AcdpException;
import com.emc.acdp.api.AcdpAdminApi;
import com.emc.acdp.api.AcdpAdminConfig;
import com.emc.acdp.api.jersey.AcdpAdminApiClient;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProvisioningTool {
    public static final String ADMIN_URI_O = "u";
    public static final String ADMIN_URI_OPTION = "admin-service-uri";
    public static final String ADMIN_URI_DESC = "Required. The URI for the ACDP admin REST service. "
                                                + "This typically resides on the authorization node. Example:\n"
                                                + "http://adminuser:password@host:port";
    public static final String ADMIN_URI_ARG_NAME = "admin-service-uri";

    public static final String ACCOUNT_ID_O = "a";
    public static final String ACCOUNT_ID_OPTION = "account-id";
    public static final String ACCOUNT_ID_DESC = "Required. The ACDP account id for which storage should be " +
                                                 "provisioned in Atmos. Example: A81136692356";
    public static final String ACCOUNT_ID_ARG_NAME = "account-id";

    public static final String SUBSCRIPTION_ID_O = "s";
    public static final String SUBSCRIPTION_ID_OPTION = "subscription-id";
    public static final String SUBSCRIPTION_ID_DESC = "Required. The subscription id for the account (this should " +
                                                      "already exist). Example: A81136692356-storageservice01";
    public static final String SUBSCRIPTION_ID_ARG_NAME = "subscription-id";

    public static final String DISABLE_SSL_VALIDATION_OPTION = "disable-ssl-validation";
    public static final String DISABLE_SSL_VALIDATION_DESC = "Disables SSL validation.\n"
                                                             + "Use this option if the admin REST service uses SSL with a self-signed certificate";

    public static final String HELP_OPTION = "help";
    public static final String HELP_DESC = "Displays this help content";

    public static final String URI_PATTERN = "^(http|https)://([a-zA-Z0-9/\\-]+):([a-zA-Z0-9\\+/=#]+)@([^/]*?)(:[0-9]+)?(?:/)?$";

    private static final Logger log = Logger.getLogger( ProvisioningTool.class );

    public static void main( String[] args ) throws Exception {
        try {
            ProvisioningTool tool = new ProvisioningTool();
            Options options = tool.getOptions();

            // parse command line
            GnuParser gnuParser = new GnuParser();
            CommandLine line = null;
            try {
                line = gnuParser.parse( options, args );
            } catch ( ParseException e ) {
                System.err.println( e.getMessage() );
                help( options );
                System.exit( 2 );
            }

            // Special check for help
            if ( line.hasOption( HELP_OPTION ) ) {
                help( options );
                System.exit( 0 );
            }

            if ( !tool.parse( line ) ) {
                help( options );
                System.exit( 1 );
            }

            Date start = new Date();

            tool.execute();

            Date end = new Date();
            Calendar runTime = Calendar.getInstance();
            runTime.setTime( new Date( end.getTime() - start.getTime() ) );
            System.out.println( "\nTool completed in " + runTime.get( Calendar.MINUTE ) + "m, "
                                + runTime.get( Calendar.SECOND ) + "s" );

        } catch ( AcdpException e ) {
            System.out.println( e.getMessage() );
            if ( e.getAcdpCode() != null )
                System.out.println( "Error code " + e.getAcdpCode() );
            else
                System.out.println( "HTTP code " + e.getHttpCode() );
        } catch ( Throwable t ) {
            log.error( "Unexpected error", t );
            System.out.println( "Error: " + t.getMessage() );
        }
    }

    private static void help( Options options ) {
        HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp( "java -jar ProvisioningTool.jar <options ...>\nOptions:",
                       options );
    }

    private AcdpAdminApi adminApi;
    private String accountId;
    private String subscriptionId;

    private void execute() throws IOException {
        adminApi.provisionSubscription( accountId, subscriptionId, false );
    }

    private boolean parse( CommandLine line ) throws URISyntaxException {
        if ( !line.hasOption( ADMIN_URI_OPTION ) || !line.hasOption( ACCOUNT_ID_OPTION ) || !line.hasOption(
                SUBSCRIPTION_ID_OPTION ) ) return false;

        AcdpAdminConfig config = new AcdpAdminConfig();
        Pattern p = Pattern.compile( URI_PATTERN );
        String source = line.getOptionValue( ADMIN_URI_OPTION );
        Matcher m = p.matcher( source );
        if ( !m.matches() ) {
            log.debug( source + " does not match " + p );
            return false;
        }
        config.setProto( m.group( 1 ) );
        config.setUsername( m.group( 2 ) );
        config.setPassword( m.group( 3 ) );
        config.setHost( m.group( 4 ) );
        String sPort = null;
        if ( m.groupCount() == 5 ) {
            sPort = m.group( 5 );
        }
        if ( sPort != null ) {
            config.setPort( Integer.parseInt( sPort.substring( 1 ) ) );
        }

        accountId = line.getOptionValue( ACCOUNT_ID_OPTION );

        subscriptionId = line.getOptionValue( SUBSCRIPTION_ID_OPTION );

        if ( line.hasOption( DISABLE_SSL_VALIDATION_OPTION ) ) config.setDisableSslValidation( true );

        adminApi = new AcdpAdminApiClient( config );

        return true;
    }

    @SuppressWarnings( "static-access" )
    private Options getOptions() {
        Options opts = new Options();
        opts.addOption( OptionBuilder.withDescription( ADMIN_URI_DESC ).isRequired()
                                     .withLongOpt( ADMIN_URI_OPTION )
                                     .hasArg().withArgName( ADMIN_URI_ARG_NAME ).create( ADMIN_URI_O ) );
        opts.addOption( OptionBuilder.withDescription( ACCOUNT_ID_DESC ).isRequired()
                                     .withLongOpt( ACCOUNT_ID_OPTION )
                                     .hasArg().withArgName( ACCOUNT_ID_ARG_NAME ).create( ACCOUNT_ID_O ) );
        opts.addOption( OptionBuilder.withDescription( SUBSCRIPTION_ID_DESC ).isRequired()
                                     .withLongOpt( SUBSCRIPTION_ID_OPTION )
                                     .hasArg().withArgName( SUBSCRIPTION_ID_ARG_NAME ).create( SUBSCRIPTION_ID_O ) );
        opts.addOption( OptionBuilder.withDescription( DISABLE_SSL_VALIDATION_DESC )
                                     .withLongOpt( DISABLE_SSL_VALIDATION_OPTION ).create() );
        opts.addOption( OptionBuilder.withDescription( HELP_DESC )
                                     .withLongOpt( HELP_OPTION ).create() );
        return opts;
    }

    public AcdpAdminApi getAdminApi() {
        return adminApi;
    }

    public void setAdminApi( AcdpAdminApi adminApi ) {
        this.adminApi = adminApi;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId( String accountId ) {
        this.accountId = accountId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId( String subscriptionId ) {
        this.subscriptionId = subscriptionId;
    }
}
