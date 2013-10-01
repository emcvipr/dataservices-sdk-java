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

import com.emc.acdp.api.AcdpAdminApi;
import com.emc.acdp.AcdpException;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AcdpReport {
    private static final Logger log = Logger.getLogger( AcdpReport.class );

    protected AcdpAdminApi adminApi;

    private int threadCount = 10;
    private Renderer renderer;

    private Queue<ReportRow> filledRowQueue = new PriorityBlockingQueue<ReportRow>();
    private Set<StatusListener> listeners = new HashSet<StatusListener>();
    private int filledRowCount = 0;

    public AcdpReport() {
    }

    /**
     * Returns the identifying names for all columns in display order.
     */
    protected abstract String[] getColumnNames();

    /**
     * Returns any report notes that will appear above the header. Default implementation returns null
     */
    protected List<Map<String, String>> getReportNotes() {
        return null;
    }

    /**
     * Returns a map of column names to column labels for rendering.
     */
    protected abstract Map<String, String> getHeaderLabels();

    /**
     * Returns seed data for the report. These rows have enough key data to populate the whole report with
     * additional queries. The final row count may expand beyond this list (see {#fillRow}).
     */
    protected abstract List<ReportRow> getSeedData();

    /**
     * Fills a report row with remaining data (that was not part of, but is queried by the seed data). There may be
     * cases where a single row of seed data will expand into multiple rows of filled data, thus the return type of
     * list. However, the returned set of rows should only represent the key data in the seed row. In most cases, this
     * list will only contain the initial row.
     * <p/>
     * Use <code>ReportRow.setSubIndex()</code> to specify the render order if the returned list has multiple rows.
     * <p/>
     * You may want to clone the seed row to keep memory footprint low since all seed rows are in
     * memory for the duration of the report run. Cloning also makes sense if you're rows may multiply.
     */
    protected abstract List<ReportRow> fillRow( ReportRow seededRow );

    /**
     * Since the renderer is associated with a single output stream, DO NOT call this method more than once per object
     * without first setting a new renderer.
     */
    public void run() throws IOException {
        renderer.setColumnNames( getColumnNames() );
        if ( getReportNotes() != null ) {
            for ( Map<String, String> noteRow : getReportNotes() ) {
                renderer.renderHeader( noteRow );
            }
        }
        renderer.renderHeader( getHeaderLabels() );

        // first, create a task list for filling the report and give it to an executor to manage threads.
        final AcdpReport report = this;
        List<ReportRow> reportRows = getSeedData();
        final int reportSize = reportRows.size();
        int renderIndex = 0;
        ThreadPoolExecutor executor = new ThreadPoolExecutor( threadCount,
                                                              threadCount,
                                                              0,
                                                              TimeUnit.SECONDS,
                                                              new LinkedBlockingQueue<Runnable>() );
        try {
            for ( final ReportRow row : reportRows ) {
                Thread fillerThread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            // each filler thread fills the row and adds it to the "done" queue
                            filledRowQueue.addAll( fillRow( row ) );
                            log.info( "Filled seed row " + row.getIndex() );
                        } catch ( Throwable t ) {
                            log.error( "Error filling seed row " + row.getIndex(), t );
                            filledRowQueue.add( row );
                        }
                        int count = incrementFilledRowCount();
                        sendStatusEvent( new StatusEvent( report,
                                                          "Filled " + count + " of " + reportSize
                                                          + " seed rows",
                                                          ((float) count) / ((float) reportSize) ) );
                    }
                };
                executor.execute( fillerThread );
            }

            // then, wait to render any filled data rows in the appropriate order as they are ready
            log.debug( "Starting monitor loop to render rows as they're filled" );
            do {
                LogMF.debug( log,
                             "Monitor loop running (activeCount: {0}, filled rows: {1}, renderIndex: {2}, nextFilledIndex: {3})",
                             executor.getActiveCount(),
                             filledRowQueue.size(),
                             renderIndex,
                             filledRowQueue.isEmpty() ? "N/A" : filledRowQueue.peek().getIndex() );
                while ( !filledRowQueue.isEmpty() && filledRowQueue.peek().getIndex() <= renderIndex ) {

                    // if the next row index to render is ready in the queue, remove it and render it, then set the next
                    // row index to render
                    ReportRow row = filledRowQueue.remove();
                    LogMF.debug( log, "Rendering row index {0}.{1}", row.getIndex(), row.getSubIndex() );
                    renderer.renderRow( row );
                    renderIndex = row.getIndex() + 1;
                }
                try {
                    log.debug( "Monitor loop sleeping" );
                    Thread.sleep( 500 );
                } catch ( InterruptedException e ) {
                    log.warn( "I was interrupted!", e );
                }
                LogMF.debug( log, "Active thread count: {0}", executor.getActiveCount() );
            } while ( executor.getActiveCount() > 0 || (!filledRowQueue.isEmpty()
                                                        && filledRowQueue.peek().getIndex() <= renderIndex) );
        } finally {
            // shutdown thread pool
            log.debug( "Shutting down executor" );
            executor.shutdown();
        }

        // finish render
        log.debug( "Calling render.done()" );
        renderer.done();

        if ( !filledRowQueue.isEmpty() ) {
            // somehow we're missing rows
            throw new AcdpException(
                    "Filler threads are idle, but I'm still waiting for seed row #" + renderIndex + " to be filled.\n" +
                    "Next row in filled queue is #" + filledRowQueue.peek().getIndex() );
        }

        log.info( "Report complete" );
    }

    public AcdpAdminApi getAdminApi() {
        return adminApi;
    }

    public void setAdminApi( AcdpAdminApi adminApi ) {
        this.adminApi = adminApi;
    }

    public Renderer getRenderer() {
        return renderer;
    }

    public void setRenderer( Renderer renderer ) {
        this.renderer = renderer;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount( int threadCount ) {
        this.threadCount = threadCount;
    }

    public void addStatusListener( StatusListener listener ) {
        this.listeners.add( listener );
    }

    public void removeStatusListener( StatusListener listener ) {
        this.listeners.remove( listener );
    }

    protected String concatNullsLast( Object... values ) {
        StringBuilder sb = new StringBuilder();
        for ( Object val : values ) {
            if ( val == null || val.toString().length() == 0 ) sb.append( "~" );
            else sb.append( val.toString() );
            sb.append( ":" );
        }
        return sb.toString();
    }

    protected void sendStatusEvent( StatusEvent event ) {
        for ( StatusListener listener : listeners ) {
            listener.statusUpdated( event );
        }
    }

    private synchronized int incrementFilledRowCount() {
        return ++filledRowCount;
    }
}
