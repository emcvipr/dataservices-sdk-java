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
import com.emc.acdp.api.AcdpAdminApi;
import com.emc.acdp.api.AcdpAdminConfig;
import com.emc.acdp.api.jersey.AcdpAdminApiClient;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AcdpReportGenerator implements StatusListener {
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

    public static final String HELP_OPTION = "help";
    public static final String HELP_DESC = "Displays this help content";

    public static final String URI_PATTERN = "^(http|https)://([a-zA-Z0-9/\\-]+):([a-zA-Z0-9\\+/=#]+)@([^/]*?)(:[0-9]+)?(?:/)?$";

    private static final Logger log = Logger.getLogger( AcdpReportGenerator.class );

    private static final NumberFormat percentFormat = NumberFormat.getPercentInstance();

    static {
        percentFormat.setMinimumIntegerDigits( 2 );
        percentFormat.setMinimumFractionDigits( 2 );
        percentFormat.setMaximumFractionDigits( 2 );
    }

    public static void main( String[] args ) throws Exception {
        try {
            AcdpReportGenerator generator = new AcdpReportGenerator();
            Options options = generator.getOptions();

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

            if ( !generator.parse( line ) ) {
                help( options );
                System.exit( 1 );
            }

            generator.setUserReportFile( new File( "summary_user_report.csv" ) );
            generator.setUsageReportFile( new File( "summary_usage_report.csv" ) );
            generator.setLogStatusToConsole( true );

            Date start = new Date();

            generator.generateUserReport();
            generator.generateUsageReport();

            Date end = new Date();
            Calendar runTime = Calendar.getInstance();
            runTime.setTime( new Date( end.getTime() - start.getTime() ) );
            System.out
                  .println( "\nReports completed in " + runTime.get( Calendar.MINUTE ) + "m, "
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
        fmt.printHelp( "java -jar AcdpReportGenerator.jar --admin-service <admin-service-uri> [options ...]\nOptions:",
                       options );
    }

    private AcdpAdminApi adminApi;
    private File usageReportFile;
    private File userReportFile;
    private int threadCount = 10;
    private boolean logStatusToConsole = false;

    public void generateUsageReport() throws IOException {
        runReport( new SummaryUsageReport(), usageReportFile );
    }

    public void generateUserReport() throws IOException {
        runReport( new SummaryUsersReport(), userReportFile );
    }

    private void runReport( AcdpReport report, File reportFile ) throws IOException {
        OutputStream fos = new FileOutputStream( reportFile );
        try {
            report.setAdminApi( adminApi );
            report.setRenderer( new CsvRenderer( fos ) );
            report.setThreadCount( threadCount );

            report.addStatusListener( this );

            report.run();
        } finally {
            if ( logStatusToConsole ) System.out.println(); // don't overwrite our status line
            fos.close();
        }
    }

    @Override
    public void statusUpdated( StatusEvent event ) {
        if ( !logStatusToConsole ) return;

        //String status = event.getCurrentTask();
        String reportName = event.getSource().getClass().getSimpleName();
        String status = "Filling " + reportName + " data...";
        if ( status.length() > 60 )
            status = status.substring( 0, 57 ) + "...";

        StringBuilder builder = new StringBuilder( status );
        if ( status.length() < 60 ) {
            for ( int i = 0; i < (60 - status.length()); i++ ) {
                builder.append( " " );
            }
        }
        builder.append( percentFormat.format( event.getPercentComplete() ) );
        builder.append( " complete." );

        System.out.print( "\r" + builder );
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

        if ( line.hasOption( THREAD_POOL_SIZE_OPTION ) ) {
            threadCount = Integer.parseInt( line.getOptionValue( THREAD_POOL_SIZE_OPTION ) );
        }

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
        opts.addOption( OptionBuilder.withDescription( THREAD_POOL_SIZE_DESC )
                                     .withLongOpt( THREAD_POOL_SIZE_OPTION )
                                     .hasArg().withArgName( THREAD_POOL_SIZE_ARG_NAME ).create() );
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

    public File getUsageReportFile() {
        return usageReportFile;
    }

    public void setUsageReportFile( File usageReportFile ) {
        this.usageReportFile = usageReportFile;
    }

    public File getUserReportFile() {
        return userReportFile;
    }

    public void setUserReportFile( File userReportFile ) {
        this.userReportFile = userReportFile;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount( int threadCount ) {
        this.threadCount = threadCount;
    }

    public boolean isLogStatusToConsole() {
        return logStatusToConsole;
    }

    public void setLogStatusToConsole( boolean logStatusToConsole ) {
        this.logStatusToConsole = logStatusToConsole;
    }
}
