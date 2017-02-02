package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class ThrottlingReporterTest {

    @Test
    public void testLessThanMaxThrottles() {
        ThrottlingReporter reporter = new ThrottlingReporter(5);
        assertThat(reporter.shouldReportError(), is(false));
        reporter.throttled();
        assertThat(reporter.shouldReportError(), is(false));
    }

    @Test
    public void testMoreThanMaxThrottles() {
        ThrottlingReporter reporter = new ThrottlingReporter(1);
        assertThat(reporter.shouldReportError(), is(false));
        reporter.throttled();
        reporter.throttled();
        assertThat(reporter.shouldReportError(), is(true));
    }

    @Test
    public void testSuccessResetsErrors() {
        ThrottlingReporter reporter = new ThrottlingReporter(1);
        assertThat(reporter.shouldReportError(), is(false));
        reporter.throttled();
        reporter.throttled();
        assertThat(reporter.shouldReportError(), is(true));
        reporter.success();
        assertThat(reporter.shouldReportError(), is(false));

    }

}