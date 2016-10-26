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
