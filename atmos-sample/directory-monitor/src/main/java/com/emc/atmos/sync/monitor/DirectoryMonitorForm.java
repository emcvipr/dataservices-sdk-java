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
package com.emc.atmos.sync.monitor;

import org.apache.log4j.Logger;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class DirectoryMonitorForm extends JFrame implements ActionListener, WindowListener {
    private static final Logger log = Logger.getLogger( DirectoryMonitorForm.class );

    private static final String PROPERTIES_FILE = ".atmos.properties";
    private static final String PROP_ATMOS_HOST = "atmos.host";
    private static final String PROP_ATMOS_PORT = "atmos.port";
    private static final String PROP_ATMOS_UID = "atmos.uid";
    private static final String PROP_ATMOS_SECRET = "atmos.secret";
    private static final String PROP_ATMOS_PATH = "atmos.path";
    private static final String PROP_LOCAL_PATH = "local.path";
    private static final String PROP_RECURSIVE = "recursive";

    public static void main( String[] args ) {
        new DirectoryMonitorForm();
    }

    private JPanel mainPanel;
    private JTextField atmosPort;
    private JTextField atmosHost;
    private JTextField atmosSecret;
    private JTextField localDirectory;
    private JCheckBox recursive;
    private JButton browseButton;
    private JTextField atmosDirectory;
    private JTextField atmosUid;
    private JButton startMonitorButton;
    private JPanel greenLight;
    private JButton stopMonitorButton;

    private DirectoryMonitor monitor;

    public DirectoryMonitorForm() {
        super( "AtmosSync Directory Monitor" );
        setupUI();
        setMinimumSize( new Dimension( 512, 300 ) );
        setContentPane( mainPanel );
        browseButton.addActionListener( this );
        startMonitorButton.addActionListener( this );
        stopMonitorButton.addActionListener( this );
        monitor = new DirectoryMonitor();
        monitor.addActionListener( this );
        readProperties();
        setVisible( true );
        setDefaultCloseOperation( JFrame.EXIT_ON_CLOSE );
        addWindowListener( this );
    }

    @Override
    public void actionPerformed( ActionEvent actionEvent ) {
        if ( actionEvent instanceof SyncEvent ) {
            SyncEvent syncEvent = (SyncEvent) actionEvent;
            switch ( syncEvent.getCommand() ) {
                case START_SYNC:
                    greenLight.setBackground( Color.YELLOW );
                    break;
                case SYNC_COMPLETE:
                    greenLight.setBackground( Color.GREEN );
                    break;
                case ERROR:
                    JOptionPane.showMessageDialog( mainPanel, syncEvent.getException().getMessage(),
                            "Error", JOptionPane.ERROR_MESSAGE );
                    break;
            }

            // browse button
        } else if ( actionEvent.getSource() == browseButton ) {
            final JFileChooser chooser = new JFileChooser();
            chooser.setFileSelectionMode( JFileChooser.DIRECTORIES_ONLY );
            int returnVal = chooser.showOpenDialog( browseButton );
            if ( returnVal == JFileChooser.APPROVE_OPTION ) {
                File file = chooser.getSelectedFile();
                localDirectory.setText( file.getAbsolutePath() );
            }

            // start button
        } else if ( actionEvent.getSource() == startMonitorButton ) {
            DirectoryMonitorBean bean = new DirectoryMonitorBean();
            try {
                getData( bean );
                monitor.setMonitorBean( bean );
                monitor.startMonitor();
                greenLight.setBackground( Color.GREEN );
            } catch ( NumberFormatException e ) {
                JOptionPane.showMessageDialog( mainPanel, "Atmos port must be a number",
                        "Error", JOptionPane.ERROR_MESSAGE );
            } catch ( Exception e ) {
                JOptionPane.showMessageDialog( mainPanel, e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE );
                log.error( "exception", e );
            }

            // stop button
        } else if ( actionEvent.getSource() == stopMonitorButton ) {
            monitor.stopMonitor();
            greenLight.setBackground( mainPanel.getBackground() );
        }
    }

    public void setData( DirectoryMonitorBean data ) {
        atmosPort.setText( Integer.toString( data.getAtmosPort() ) );
        atmosHost.setText( data.getAtmosHost() );
        atmosUid.setText( data.getAtmosUid() );
        atmosSecret.setText( data.getAtmosSecret() );
        atmosDirectory.setText( data.getAtmosDirectory() );
        localDirectory.setText( data.getLocalDirectory() );
        recursive.setSelected( data.isRecursive() );
    }

    public void getData( DirectoryMonitorBean data ) {
        data.setAtmosPort( Integer.parseInt( atmosPort.getText() ) );
        data.setAtmosHost( atmosHost.getText() );
        data.setAtmosUid( atmosUid.getText() );
        data.setAtmosSecret( atmosSecret.getText() );
        data.setAtmosDirectory( atmosDirectory.getText() );
        data.setLocalDirectory( localDirectory.getText() );
        data.setRecursive( recursive.isSelected() );
    }

    @Override
    public void windowActivated( WindowEvent windowEvent ) {
    }

    @Override
    public void windowOpened( WindowEvent windowEvent ) {
    }

    @Override
    public void windowClosing( WindowEvent windowEvent ) {
        writeProperties();
    }

    @Override
    public void windowClosed( WindowEvent windowEvent ) {
    }

    @Override
    public void windowIconified( WindowEvent windowEvent ) {
    }

    @Override
    public void windowDeiconified( WindowEvent windowEvent ) {
    }

    @Override
    public void windowDeactivated( WindowEvent windowEvent ) {
    }

    private void readProperties() {
        Properties props = new Properties();
        try {
            File propFile = new File( System.getProperty( "user.home" ), PROPERTIES_FILE );
            if ( propFile.exists() && propFile.canRead() ) {
                props.load( new FileReader( propFile ) );
                DirectoryMonitorBean bean = new DirectoryMonitorBean();
                bean.setAtmosHost( props.getProperty( PROP_ATMOS_HOST ) );
                bean.setAtmosPort( Integer.parseInt( props.getProperty( PROP_ATMOS_PORT ) ) );
                bean.setAtmosUid( props.getProperty( PROP_ATMOS_UID ) );
                bean.setAtmosSecret( props.getProperty( PROP_ATMOS_SECRET ) );
                bean.setAtmosDirectory( props.getProperty( PROP_ATMOS_PATH ) );
                bean.setLocalDirectory( props.getProperty( PROP_LOCAL_PATH ) );
                bean.setRecursive( Boolean.parseBoolean( props.getProperty( PROP_RECURSIVE ) ) );
                setData( bean );
            }
        } catch ( IOException e ) {
            log.error( "error reading properties", e );
        }
    }

    private void writeProperties() {
        DirectoryMonitorBean bean = new DirectoryMonitorBean();
        getData( bean );
        Properties props = new Properties();
        props.setProperty( PROP_ATMOS_HOST, bean.getAtmosHost() );
        props.setProperty( PROP_ATMOS_PORT, Integer.toString( bean.getAtmosPort() ) );
        props.setProperty( PROP_ATMOS_UID, bean.getAtmosUid() );
        props.setProperty( PROP_ATMOS_SECRET, bean.getAtmosSecret() );
        props.setProperty( PROP_ATMOS_PATH, bean.getAtmosDirectory() );
        props.setProperty( PROP_LOCAL_PATH, bean.getLocalDirectory() );
        props.setProperty( PROP_RECURSIVE, Boolean.toString( bean.isRecursive() ) );
        try {
            File propFile = new File( System.getProperty( "user.home" ), PROPERTIES_FILE );
            props.store( new FileWriter( propFile ), "AtmosSync Directory Monitor properties" );
            propFile.setReadable( false, false );
            propFile.setReadable( true );
            propFile.setWritable( false, false );
            propFile.setWritable( true );
        } catch ( IOException e ) {
            log.error( "error writing properties", e );
        }
    }

    private void setupUI() {
        mainPanel = new JPanel();
        mainPanel.setLayout( new GridBagLayout() );
        mainPanel.setMinimumSize( new Dimension( 515, 252 ) );
        final JLabel label1 = new JLabel();
        label1.setEnabled( true );
        label1.setHorizontalAlignment( 4 );
        label1.setText( "Atmos Host:" );
        GridBagConstraints gbc;
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( label1, gbc );
        atmosPort = new JTextField();
        atmosPort.setText( "80" );
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 1;
        gbc.gridwidth = 3;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( atmosPort, gbc );
        final JLabel label2 = new JLabel();
        label2.setHorizontalAlignment( 4 );
        label2.setText( "Port:" );
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( label2, gbc );
        atmosHost = new JTextField();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.gridwidth = 3;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( atmosHost, gbc );
        final JLabel label3 = new JLabel();
        label3.setHorizontalAlignment( 4 );
        label3.setText( "UID:" );
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( label3, gbc );
        atmosUid = new JTextField();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 2;
        gbc.gridwidth = 3;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( atmosUid, gbc );
        final JLabel label4 = new JLabel();
        label4.setHorizontalAlignment( 4 );
        label4.setText( "Secret Key:" );
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( label4, gbc );
        atmosSecret = new JTextField();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 3;
        gbc.gridwidth = 3;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( atmosSecret, gbc );
        final JLabel label5 = new JLabel();
        label5.setHorizontalAlignment( 4 );
        label5.setText( "Local Directory:" );
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 5;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( label5, gbc );
        final JLabel label6 = new JLabel();
        label6.setHorizontalAlignment( 4 );
        label6.setText( "Remote Path:" );
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( label6, gbc );
        atmosDirectory = new JTextField();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 4;
        gbc.gridwidth = 3;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( atmosDirectory, gbc );
        startMonitorButton = new JButton();
        startMonitorButton.setText( "Start Monitor" );
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 8;
        gbc.weightx = 1.0;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( startMonitorButton, gbc );
        greenLight = new JPanel();
        greenLight.setLayout( new GridBagLayout() );
        greenLight.setMaximumSize( new Dimension( 20, 20 ) );
        greenLight.setMinimumSize( new Dimension( 20, 20 ) );
        greenLight.setPreferredSize( new Dimension( 20, 20 ) );
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 8;
        mainPanel.add( greenLight, gbc );
        greenLight.setBorder( BorderFactory.createTitledBorder( BorderFactory.createLineBorder( new Color( -16777216 ) ), null ) );
        stopMonitorButton = new JButton();
        stopMonitorButton.setText( "Stop Monitor" );
        gbc = new GridBagConstraints();
        gbc.gridx = 2;
        gbc.gridy = 8;
        gbc.gridwidth = 2;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( stopMonitorButton, gbc );
        final JSeparator separator1 = new JSeparator();
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 7;
        gbc.gridwidth = 4;
        gbc.weighty = 1.0;
        gbc.fill = GridBagConstraints.BOTH;
        mainPanel.add( separator1, gbc );
        localDirectory = new JTextField();
        localDirectory.setEnabled( true );
        localDirectory.setEditable( false );
        localDirectory.setMinimumSize( new Dimension( 200, 20 ) );
        localDirectory.setPreferredSize( new Dimension( 200, 20 ) );
        localDirectory.setText( "" );
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 5;
        gbc.gridwidth = 2;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( localDirectory, gbc );
        recursive = new JCheckBox();
        recursive.setEnabled( true );
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 6;
        gbc.gridwidth = 1;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        mainPanel.add( recursive, gbc );
        final JLabel label7 = new JLabel();
        label7.setHorizontalAlignment( 4 );
        label7.setText( "Recursive:" );
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 6;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( label7, gbc );
        browseButton = new JButton();
        browseButton.setText( "browse" );
        gbc = new GridBagConstraints();
        gbc.gridx = 3;
        gbc.gridy = 5;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets( 3, 3, 3, 3 );
        mainPanel.add( browseButton, gbc );
        label6.setLabelFor( atmosDirectory );
    }
}
