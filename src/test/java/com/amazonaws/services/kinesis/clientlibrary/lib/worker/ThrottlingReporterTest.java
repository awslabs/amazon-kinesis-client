package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogConfigurationException;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import lombok.RequiredArgsConstructor;

@RunWith(MockitoJUnitRunner.class)
public class ThrottlingReporterTest {

    private static final String SHARD_ID = "Shard-001";

    private static final Log throttleLog = mock(Log.class);

    @RequiredArgsConstructor
    private static class DirectRegisterLogFactory extends LogFactoryImpl {

        private final Map<Class<?>, Log> logMapping;

        public static void mockLogFactory(Map<Class<?>, Log> logMapping) {
            factories.put(Thread.currentThread().getContextClassLoader(), new DirectRegisterLogFactory(logMapping));
        }

        @Override
        public Log getInstance(Class clazz) throws LogConfigurationException {
            if (logMapping.containsKey(clazz)) {
                return logMapping.get(clazz);
            }
            return super.getInstance(clazz);
        }
    }

    @BeforeClass
    public static void beforeClass() {
        Map<Class<?>, Log> logMapping = new HashMap<>();
        logMapping.put(ThrottlingReporter.class, throttleLog);

        DirectRegisterLogFactory.mockLogFactory(logMapping);
    }

    @Before
    public void before() {
        //
        // Have to to do this since the only time that the logFactory will be able to inject a mock is on
        // class load.
        //
        Mockito.reset(throttleLog);
    }

    @Test
    public void testLessThanMaxThrottles() {
        ThrottlingReporter reporter = new ThrottlingReporter(5, SHARD_ID);
        reporter.throttled();
        verify(throttleLog).warn(any(Object.class));
        verify(throttleLog, never()).error(any(Object.class));

    }

    @Test
    public void testMoreThanMaxThrottles() {
        ThrottlingReporter reporter = new ThrottlingReporter(1, SHARD_ID);
        reporter.throttled();
        reporter.throttled();
        verify(throttleLog).warn(any(Object.class));
        verify(throttleLog).error(any(Object.class));
    }

    @Test
    public void testSuccessResetsErrors() {
        ThrottlingReporter reporter = new ThrottlingReporter(1, SHARD_ID);
        reporter.throttled();
        reporter.throttled();
        reporter.throttled();
        reporter.throttled();
        reporter.success();
        reporter.throttled();
        verify(throttleLog, times(2)).warn(any(Object.class));
        verify(throttleLog, times(3)).error(any(Object.class));

    }

}