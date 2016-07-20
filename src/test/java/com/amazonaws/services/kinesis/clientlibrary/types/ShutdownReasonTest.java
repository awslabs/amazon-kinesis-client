package com.amazonaws.services.kinesis.clientlibrary.types;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests of ShutdownReason enum class.
 */
public class ShutdownReasonTest {
    @Test
    public void testToString() {
        Assert.assertEquals("ZOMBIE", String.valueOf(ShutdownReason.ZOMBIE));
        Assert.assertEquals("TERMINATE", String.valueOf(ShutdownReason.TERMINATE));
        Assert.assertEquals("ZOMBIE", ShutdownReason.ZOMBIE.toString());
        Assert.assertEquals("TERMINATE", ShutdownReason.TERMINATE.toString());
    }

}
