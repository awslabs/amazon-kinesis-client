/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.kinesis.leases.impl;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.*;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;

public class LeaseCoordinatorExerciser {

    private static final Log LOG = LogFactory.getLog(LeaseCoordinatorExerciser.class);

    public static void main(String[] args)
        throws InterruptedException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {

        int numCoordinators = 9;
        int numLeases = 73;
        int leaseDurationMillis = 10000;
        int epsilonMillis = 100;

        AWSCredentialsProvider creds =
                new DefaultAWSCredentialsProviderChain();
        AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(creds);

        ILeaseManager<KinesisClientLease> leaseManager = new KinesisClientLeaseManager("nagl_ShardProgress", ddb);

        if (leaseManager.createLeaseTableIfNotExists(10L, 50L)) {
            LOG.info("Waiting for newly created lease table");
            if (!leaseManager.waitUntilLeaseTableExists(10, 300)) {
                LOG.error("Table was not created in time");
                return;
            }
        }

        CWMetricsFactory metricsFactory = new CWMetricsFactory(creds, "testNamespace", 30 * 1000, 1000);
        final List<LeaseCoordinator<KinesisClientLease>> coordinators =
                new ArrayList<LeaseCoordinator<KinesisClientLease>>();
        for (int i = 0; i < numCoordinators; i++) {
            String workerIdentifier = "worker-" + Integer.toString(i);

            LeaseCoordinator<KinesisClientLease> coord = new LeaseCoordinator<KinesisClientLease>(leaseManager,
                    workerIdentifier,
                    leaseDurationMillis,
                    epsilonMillis,
                    metricsFactory);

            coordinators.add(coord);
        }

        leaseManager.deleteAll();

        for (int i = 0; i < numLeases; i++) {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setLeaseKey(Integer.toString(i));
            lease.setCheckpoint(new ExtendedSequenceNumber("checkpoint"));
            leaseManager.createLeaseIfNotExists(lease);
        }

        final JFrame frame = new JFrame("Test Visualizer");
        frame.setPreferredSize(new Dimension(800, 600));
        final JPanel panel = new JPanel(new GridLayout(coordinators.size() + 1, 0));
        final JLabel ticker = new JLabel("tick");
        panel.add(ticker);
        frame.getContentPane().add(panel);

        final Map<String, JLabel> labels = new HashMap<String, JLabel>();
        for (final LeaseCoordinator<KinesisClientLease> coord : coordinators) {
            JPanel coordPanel = new JPanel();
            coordPanel.setLayout(new BoxLayout(coordPanel, BoxLayout.X_AXIS));
            final Button button = new Button("Stop " + coord.getWorkerIdentifier());
            button.setMaximumSize(new Dimension(200, 50));
            button.addActionListener(new ActionListener() {

                @Override
                public void actionPerformed(ActionEvent arg0) {
                    if (coord.isRunning()) {
                        coord.stop();
                        button.setLabel("Start " + coord.getWorkerIdentifier());
                    } else {
                        try {
                            coord.start();
                        } catch (LeasingException e) {
                            LOG.error(e);
                        }
                        button.setLabel("Stop " + coord.getWorkerIdentifier());
                    }
                }

            });
            coordPanel.add(button);

            JLabel label = new JLabel();
            coordPanel.add(label);
            labels.put(coord.getWorkerIdentifier(), label);
            panel.add(coordPanel);
        }

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        new Thread() {

            // Key is lease key, value is green-ness as a value from 0 to 255.
            // Great variable name, huh?
            private Map<String, Integer> greenNesses = new HashMap<String, Integer>();

            // Key is lease key, value is last owning worker
            private Map<String, String> lastOwners = new HashMap<String, String>();

            @Override
            public void run() {
                while (true) {
                    for (LeaseCoordinator<KinesisClientLease> coord : coordinators) {
                        String workerIdentifier = coord.getWorkerIdentifier();

                        JLabel label = labels.get(workerIdentifier);

                        List<KinesisClientLease> asgn = new ArrayList<KinesisClientLease>(coord.getAssignments());
                        Collections.sort(asgn, new Comparator<KinesisClientLease>() {

                            @Override
                            public int compare(KinesisClientLease arg0, KinesisClientLease arg1) {
                                return arg0.getLeaseKey().compareTo(arg1.getLeaseKey());
                            }

                        });

                        StringBuilder builder = new StringBuilder();
                        builder.append("<html>");
                        builder.append(workerIdentifier).append(":").append(asgn.size()).append("          ");

                        for (KinesisClientLease lease : asgn) {
                            String leaseKey = lease.getLeaseKey();
                            String lastOwner = lastOwners.get(leaseKey);

                            // Color things green when they switch owners, decay the green-ness over time.
                            Integer greenNess = greenNesses.get(leaseKey);
                            if (greenNess == null || lastOwner == null || !lastOwner.equals(lease.getLeaseOwner())) {
                                greenNess = 200;
                            } else {
                                greenNess = Math.max(0, greenNess - 20);
                            }
                            greenNesses.put(leaseKey, greenNess);
                            lastOwners.put(leaseKey, lease.getLeaseOwner());

                            builder.append(String.format("<font color=\"%s\">%03d</font>",
                                    String.format("#00%02x00", greenNess),
                                    Integer.parseInt(leaseKey))).append(" ");
                        }
                        builder.append("</html>");

                        label.setText(builder.toString());
                        label.revalidate();
                        label.repaint();
                    }

                    if (ticker.getText().equals("tick")) {
                        ticker.setText("tock");
                    } else {
                        ticker.setText("tick");
                    }

                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                    }
                }
            }

        }.start();

        frame.pack();
        frame.setVisible(true);

        for (LeaseCoordinator<KinesisClientLease> coord : coordinators) {
            coord.start();
        }
    }
}
