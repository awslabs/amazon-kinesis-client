package software.amazon.kinesis.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.kinesis.utils.Cgroup.getAvailableCpusFromEffectiveCpuSet;

public class CgroupTest {

    @Test
    void test_getAvailableCpusFromEffectiveCpuSet() {
        assertEquals(8, getAvailableCpusFromEffectiveCpuSet("0-7"));
        assertEquals(9, getAvailableCpusFromEffectiveCpuSet("0-4,6,8-10"));
        assertEquals(4, getAvailableCpusFromEffectiveCpuSet("0,6,8,10"));
        assertEquals(5, getAvailableCpusFromEffectiveCpuSet("1-2,8,10,11"));
        assertEquals(1, getAvailableCpusFromEffectiveCpuSet("0"));
    }
}
