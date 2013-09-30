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
import com.emc.cdp.services.rest.model.Account;
import com.emc.cdp.services.rest.model.Identity;
import com.emc.cdp.services.rest.model.Profile;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OrphanedUserTool {
    public static final String ADMIN_URI_OPTION = "admin-service";
    public static final String ADMIN_URI_DESC = "The URI for the ACDP admin REST service.  "
                                                + "This typically resides on the authorization node.  Example:\n"
                                                + "http://adminuser:password@host:port";
    public static final String ADMIN_URI_ARG_NAME = "admin-service-uri";

    public static final String THREAD_POOL_SIZE_OPTION = "thread-pool-size";
    public static final String THREAD_POOL_SIZE_DESC =
            "Sets the number of threads to use when filling the report.  Defaults to 10.  " +
            "Increase this number to 20 for large CDP deployments with many accounts.";
    public static final String THREAD_POOL_SIZE_ARG_NAME = "number-of-threads";

    public static final String DISABLE_SSL_VALIDATION_OPTION = "disable-ssl-validation";
    public static final String DISABLE_SSL_VALIDATION_DESC = "Disables SSL validation.\n"
                                                             + "Use this option if the admin REST service uses SSL with a self-signed certificate";

    public static final String DELETE_USERS_OPTION = "delete-users";
    public static final String DELETE_USERS_DESC = "Deletes all orphaned users found.\n"
                                                   + "Use this option after you've confirmed that all users returned are unwanted and should be deleted";

    public static final String HELP_OPTION = "help";
    public static final String HELP_DESC = "Displays this help content";

    public static final String URI_PATTERN = "^(http|https)://([a-zA-Z0-9/\\-]+):([a-zA-Z0-9\\+/=#]+)@([^/]*?)(:[0-9]+)?(?:/)?$";

    private static final Logger log = Logger.getLogger( OrphanedUserTool.class );

    private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>();
    private static final String DATE_FORMAT = "yyyy-MM-dd hh:mma";

    public static void main( String[] args ) throws Exception {
        try {
            OrphanedUserTool tool = new OrphanedUserTool();
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

            tool.execute();

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
        fmt.printHelp( "java -jar OrphanedUserTool.jar --admin-service <admin-service-uri> [options ...]\nOptions:",
                       options );
    }

    private AcdpAdminApi adminApi;
    private int threadCount = 10;
    private boolean deleteUsers = false;
    private int orphanCount = 0, deleteCount = 0;

    private void execute() {
        Date start = new Date();

        List<Identity> identities = adminApi.listIdentities( true, true, 1, 1000000 ).getIdentities();
        ThreadPoolExecutor executor = new ThreadPoolExecutor( threadCount,
                                                              threadCount,
                                                              0,
                                                              TimeUnit.SECONDS,
                                                              new LinkedBlockingQueue<Runnable>() );
        log.info( "--- Found " + identities.size() + " total identities ---" );
        log.info( "Searching for orphaned identities (identities with no associated account)..." );
        if ( !deleteUsers )
            System.out.println( "Username,Last Name,First Name,Sign-up Date,E-mail,Company,City,State" );
        try {
            for ( final Identity identity : identities ) {
                Thread accountThread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            // get account info for identity
                            Account account = adminApi.getIdentityAccount( identity.getId(), false );
                            log( "Found account (" + account.getName() + ") for identity (" + identity.getId() + ")",
                                 Level.INFO, null );
                        } catch ( AcdpException e ) {
                            if ( !"AccountNotExistForIdentity".equals( e.getAcdpCode() ) ) throw e;
                            log( "No account found for identity (" + identity.getId() + ")", Level.INFO, null );
                            incrementOrphanCount();
                            if ( deleteUsers ) {
                                log( "Deleing identity (" + identity.getId() + ")", Level.INFO, null );
                                System.out.println( "Deleing identity (" + identity.getId() + ")" );
                                adminApi.deleteIdentity( identity.getId() );
                                incrementDeleteCount();
                            } else {
                                Profile profile = identity.getProfile();
                                String signUpDate = "";
                                if ( identity.getSignUpTime() != null )
                                    signUpDate = getFormat().format( identity.getSignUpTime()
                                                                             .toGregorianCalendar()
                                                                             .getTime() );
                                if ( profile == null ) profile = new Profile();
                                System.out.println( MessageFormat.format( "{0},{1},{2},{3},{4},{5},{6},{7}",
                                                                          identity.getId(),
                                                                          profile.getLastName(), profile.getFirstName(),
                                                                          signUpDate,
                                                                          profile.getEmail(), profile.getCompanyName(),
                                                                          profile.getCity(), profile.getStateCode() ) );
                            }
                        }
                    }
                };
                executor.execute( accountThread );
            }

            // wait until all threads are complete
            do {
                try {
                    log.debug( "Monitor loop sleeping" );
                    Thread.sleep( 500 );
                } catch ( InterruptedException e ) {
                    log.warn( "I was interrupted!", e );
                }
                LogMF.debug( log, "Active thread count: {0}", executor.getActiveCount() );
            } while ( executor.getActiveCount() > 0 );
        } finally {
            // shutdown thread pool
            log.debug( "Shutting down executor" );
            executor.shutdown();
        }

        Date end = new Date();
        Calendar runTime = Calendar.getInstance();
        runTime.setTime( new Date( end.getTime() - start.getTime() ) );
        log.info( "\nTool completed in " + runTime.get( Calendar.MINUTE ) + "m, "
                  + runTime.get( Calendar.SECOND ) + "s." );
        log.info( orphanCount + " orphaned users found.  " + deleteCount + " users deleted." );
    }

    private boolean parse( CommandLine line ) throws URISyntaxException {
        if ( !line.hasOption( ADMIN_URI_OPTION ) ) return false;

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

        adminApi = new AcdpAdminApiClient( config );

        if ( line.hasOption( THREAD_POOL_SIZE_OPTION ) )
            threadCount = Integer.parseInt( line.getOptionValue( THREAD_POOL_SIZE_OPTION ) );

        if ( line.hasOption( DELETE_USERS_OPTION ) ) deleteUsers = true;

        return true;
    }

    @SuppressWarnings( "static-access" )
    private Options getOptions() {
        Options opts = new Options();
        opts.addOption( OptionBuilder.withDescription( ADMIN_URI_DESC )
                                     .withLongOpt( ADMIN_URI_OPTION )
                                     .hasArg().withArgName( ADMIN_URI_ARG_NAME ).create() );
        opts.addOption( OptionBuilder.withDescription( DISABLE_SSL_VALIDATION_DESC )
                                     .withLongOpt( DISABLE_SSL_VALIDATION_OPTION ).create() );
        opts.addOption( OptionBuilder.withDescription( DELETE_USERS_DESC )
                                     .withLongOpt( DELETE_USERS_OPTION ).create() );
        opts.addOption( OptionBuilder.withDescription( THREAD_POOL_SIZE_DESC )
                                     .withLongOpt( THREAD_POOL_SIZE_OPTION )
                                     .hasArg().withArgName( THREAD_POOL_SIZE_ARG_NAME ).create() );
        opts.addOption( OptionBuilder.withDescription( HELP_DESC )
                                     .withLongOpt( HELP_OPTION ).create() );
        return opts;
    }

    private synchronized void log( String message, Priority priority, Throwable throwable ) {
        if ( throwable != null ) log.log( priority, message, throwable );
        else log.log( priority, message );
    }

    private synchronized void incrementOrphanCount() {
        orphanCount++;
    }

    private synchronized void incrementDeleteCount() {
        deleteCount++;
    }

    private static DateFormat getFormat() {
        DateFormat format = dateFormat.get();
        if ( format == null ) {
            format = new SimpleDateFormat( DATE_FORMAT );
            format.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
            dateFormat.set( format );
        }
        return format;
    }

    public AcdpAdminApi getAdminApi() {
        return adminApi;
    }

    public void setAdminApi( AcdpAdminApi adminApi ) {
        this.adminApi = adminApi;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount( int threadCount ) {
        this.threadCount = threadCount;
    }

    public boolean isDeleteUsers() {
        return deleteUsers;
    }

    public void setDeleteUsers( boolean deleteUsers ) {
        this.deleteUsers = deleteUsers;
    }
}
