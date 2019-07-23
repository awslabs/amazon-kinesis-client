package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

public interface ShardSyncer {

    void checkAndCreateLeasesForNewShards(
            IKinesisProxy kinesisProxy,
            ILeaseManager<KinesisClientLease> leaseManager,
            InitialPositionInStreamExtended initialPositionInStream,
            boolean cleanupLeasesOfCompletedShards,
            boolean ignoreUnexpectedChildShards)
            throws
            DependencyException,
            InvalidStateException,
            ProvisionedThroughputException,
            KinesisClientLibIOException;
}
