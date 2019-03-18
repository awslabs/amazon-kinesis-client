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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IPreparedCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

/**
 * A special IPreparedCheckpointer that does nothing, which can be used when preparing a checkpoint at the current
 * checkpoint sequence number where it is never necessary to do another checkpoint.
 * This simplifies programming by preventing application developers from having to reason about whether
 * their application has processed records before calling prepareCheckpoint
 *
 * Here's why it's safe to do nothing:
 * The only way to checkpoint at current checkpoint value is to have a record processor that gets
 * initialized, processes 0 records, then calls prepareCheckpoint(). The value in the table is the same, so there's
 * no reason to overwrite it with another copy of itself.
 */
public class DoesNothingPreparedCheckpointer implements IPreparedCheckpointer {

    private final ExtendedSequenceNumber sequenceNumber;

    /**
     * Constructor.
     * @param sequenceNumber the sequence number value
     */
    public DoesNothingPreparedCheckpointer(ExtendedSequenceNumber sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber getPendingCheckpoint() {
        return sequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkpoint()
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
            IllegalArgumentException {
        // This method does nothing
    }

}

