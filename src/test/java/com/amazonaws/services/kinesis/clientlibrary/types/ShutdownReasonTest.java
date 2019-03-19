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
package com.amazonaws.services.kinesis.clientlibrary.types;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import org.junit.Test;

/**
 * Unit tests of ShutdownReason enum class.
 */
public class ShutdownReasonTest {

    @Test
    public void testTransitionZombie() {
        assertThat(ShutdownReason.ZOMBIE.canTransitionTo(ShutdownReason.TERMINATE), equalTo(false));
        assertThat(ShutdownReason.ZOMBIE.canTransitionTo(ShutdownReason.REQUESTED), equalTo(false));
    }

    @Test
    public void testTransitionTerminate() {
        assertThat(ShutdownReason.TERMINATE.canTransitionTo(ShutdownReason.ZOMBIE), equalTo(true));
        assertThat(ShutdownReason.TERMINATE.canTransitionTo(ShutdownReason.REQUESTED), equalTo(false));
    }

    @Test
    public void testTransitionRequested() {
        assertThat(ShutdownReason.REQUESTED.canTransitionTo(ShutdownReason.ZOMBIE), equalTo(true));
        assertThat(ShutdownReason.REQUESTED.canTransitionTo(ShutdownReason.TERMINATE), equalTo(true));
    }

}
