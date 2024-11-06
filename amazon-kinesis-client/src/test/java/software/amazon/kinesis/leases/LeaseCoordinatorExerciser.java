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
package software.amazon.kinesis.leases;

import java.awt.Button;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

import lombok.extern.slf4j.Slf4j;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.CloudWatchMetricsFactory;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class LeaseCoordinatorExerciser {
    private static final int MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;
    private static final int MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;
    private static final int MAX_LEASE_RENEWER_THREAD_COUNT = 20;
    private static final MetricsLevel METRICS_LEVEL = MetricsLevel.DETAILED;
    private static final int FLUSH_SIZE = 200;
    private static final long INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
    private static final long INITIAL_LEASE_TABLE_WRITE_CAPACITY = 50L;

    public static void main(String[] args)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        int numCoordinators = 9;
        int numLeases = 73;
        int leaseDurationMillis = 10000;
        int epsilonMillis = 100;

        DynamoDbAsyncClient dynamoDBClient = DynamoDbAsyncClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                "nagl_ShardProgress",
                dynamoDBClient,
                new DynamoDBLeaseSerializer(),
                true,
                TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
                new DdbTableConfig(),
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
                DefaultSdkAutoConstructList.getInstance());

        MigrationAdaptiveLeaseAssignmentModeProvider mockModeProvider =
                mock(MigrationAdaptiveLeaseAssignmentModeProvider.class, Mockito.RETURNS_MOCKS);
        when(mockModeProvider.getLeaseAssignmentMode())
                .thenReturn(LeaseAssignmentMode.WORKER_UTILIZATION_AWARE_ASSIGNMENT);
        when(mockModeProvider.dynamicModeChangeSupportNeeded()).thenReturn(false);

        if (leaseRefresher.createLeaseTableIfNotExists()) {
            log.info("Waiting for newly created lease table");
            if (!leaseRefresher.waitUntilLeaseTableExists(10, 300)) {
                log.error("Table was not created in time");
                return;
            }
        }

        CloudWatchAsyncClient client = CloudWatchAsyncClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        CloudWatchMetricsFactory metricsFactory = new CloudWatchMetricsFactory(
                client,
                "testNamespace",
                30 * 1000,
                1000,
                METRICS_LEVEL,
                MetricsConfig.METRICS_DIMENSIONS_ALL,
                FLUSH_SIZE);
        final List<LeaseCoordinator> coordinators = new ArrayList<>();
        for (int i = 0; i < numCoordinators; i++) {
            String workerIdentifier = "worker-" + Integer.toString(i);

            LeaseCoordinator coord = new DynamoDBLeaseCoordinator(
                    leaseRefresher,
                    workerIdentifier,
                    leaseDurationMillis,
                    LeaseManagementConfig.DEFAULT_ENABLE_PRIORITY_LEASE_ASSIGNMENT,
                    epsilonMillis,
                    MAX_LEASES_FOR_WORKER,
                    MAX_LEASES_TO_STEAL_AT_ONE_TIME,
                    MAX_LEASE_RENEWER_THREAD_COUNT,
                    INITIAL_LEASE_TABLE_READ_CAPACITY,
                    INITIAL_LEASE_TABLE_WRITE_CAPACITY,
                    metricsFactory,
                    new LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig(),
                    LeaseManagementConfig.GracefulLeaseHandoffConfig.builder().build(),
                    new ConcurrentHashMap<>());

            coordinators.add(coord);
        }

        leaseRefresher.deleteAll();

        for (int i = 0; i < numLeases; i++) {
            Lease lease = new Lease();
            lease.leaseKey(Integer.toString(i));
            lease.checkpoint(new ExtendedSequenceNumber("checkpoint"));
            leaseRefresher.createLeaseIfNotExists(lease);
        }

        final JFrame frame = new JFrame("Test Visualizer");
        frame.setPreferredSize(new Dimension(800, 600));
        final JPanel panel = new JPanel(new GridLayout(coordinators.size() + 1, 0));
        final JLabel ticker = new JLabel("tick");
        panel.add(ticker);
        frame.getContentPane().add(panel);

        final Map<String, JLabel> labels = new HashMap<String, JLabel>();
        for (final LeaseCoordinator coord : coordinators) {
            JPanel coordPanel = new JPanel();
            coordPanel.setLayout(new BoxLayout(coordPanel, BoxLayout.X_AXIS));
            final Button button = new Button("Stop " + coord.workerIdentifier());
            button.setMaximumSize(new Dimension(200, 50));
            button.addActionListener(new ActionListener() {

                @Override
                public void actionPerformed(ActionEvent arg0) {
                    if (coord.isRunning()) {
                        coord.stop();
                        button.setLabel("Start " + coord.workerIdentifier());
                    } else {
                        try {
                            coord.start(mockModeProvider);
                        } catch (LeasingException e) {
                            log.error("{}", e);
                        }
                        button.setLabel("Stop " + coord.workerIdentifier());
                    }
                }
            });
            coordPanel.add(button);

            JLabel label = new JLabel();
            coordPanel.add(label);
            labels.put(coord.workerIdentifier(), label);
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
                    for (LeaseCoordinator coord : coordinators) {
                        String workerIdentifier = coord.workerIdentifier();

                        JLabel label = labels.get(workerIdentifier);

                        List<Lease> asgn = new ArrayList<>(coord.getAssignments());
                        Collections.sort(asgn, new Comparator<Lease>() {

                            @Override
                            public int compare(final Lease arg0, final Lease arg1) {
                                return arg0.leaseKey().compareTo(arg1.leaseKey());
                            }
                        });

                        StringBuilder builder = new StringBuilder();
                        builder.append("<html>");
                        builder.append(workerIdentifier)
                                .append(":")
                                .append(asgn.size())
                                .append("          ");

                        for (Lease lease : asgn) {
                            String leaseKey = lease.leaseKey();
                            String lastOwner = lastOwners.get(leaseKey);

                            // Color things green when they switch owners, decay the green-ness over time.
                            Integer greenNess = greenNesses.get(leaseKey);
                            if (greenNess == null || lastOwner == null || !lastOwner.equals(lease.leaseOwner())) {
                                greenNess = 200;
                            } else {
                                greenNess = Math.max(0, greenNess - 20);
                            }
                            greenNesses.put(leaseKey, greenNess);
                            lastOwners.put(leaseKey, lease.leaseOwner());

                            builder.append(String.format(
                                            "<font color=\"%s\">%03d</font>",
                                            String.format("#00%02x00", greenNess), Integer.parseInt(leaseKey)))
                                    .append(" ");
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

        for (LeaseCoordinator coord : coordinators) {
            coord.start(mockModeProvider);
        }
    }
}
