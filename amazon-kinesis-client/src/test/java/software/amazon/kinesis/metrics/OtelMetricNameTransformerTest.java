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
package software.amazon.kinesis.metrics;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link OtelMetricNameTransformer}, which is package-private.
 * Verifies the 8-step transformation pipeline: special-case map, colon substitution,
 * trailing .Time → .duration, strip trailing .Count, PascalCase splitting,
 * lowercase, namespace prefix, and validation.
 */
@RunWith(MockitoJUnitRunner.class)
public class OtelMetricNameTransformerTest {

    private static final String PREFIX = "aws.kinesis.client.";

    // -----------------------------------------------------------------------
    // Step 1: Special-case map — all 11 entries
    // -----------------------------------------------------------------------

    @Test
    public void testSpecialCase_MillisBehindLatest() {
        assertEquals(PREFIX + "consumer.lag.duration", OtelMetricNameTransformer.transformName("MillisBehindLatest"));
    }

    @Test
    public void testSpecialCase_DataBytesProcessed() {
        assertEquals(PREFIX + "records.size", OtelMetricNameTransformer.transformName("DataBytesProcessed"));
    }

    @Test
    public void testSpecialCase_ActiveStreamsCount() {
        assertEquals(PREFIX + "streams.active", OtelMetricNameTransformer.transformName("ActiveStreams.Count"));
    }

    @Test
    public void testSpecialCase_StreamsPendingDeletionCount() {
        assertEquals(
                PREFIX + "streams.pending_deletion",
                OtelMetricNameTransformer.transformName("StreamsPendingDeletion.Count"));
    }

    @Test
    public void testSpecialCase_NonExistingStreamDeleteCount() {
        assertEquals(
                PREFIX + "streams.deleted_nonexistent",
                OtelMetricNameTransformer.transformName("NonExistingStreamDelete.Count"));
    }

    @Test
    public void testSpecialCase_DeletedStreamsCount() {
        assertEquals(PREFIX + "streams.deleted", OtelMetricNameTransformer.transformName("DeletedStreams.Count"));
    }

    @Test
    public void testSpecialCase_NumStreamsToSync() {
        assertEquals(PREFIX + "streams.pending_sync", OtelMetricNameTransformer.transformName("NumStreamsToSync"));
    }

    @Test
    public void testSpecialCase_QueueSize() {
        assertEquals(
                PREFIX + "stream_id_cache.queue.size", OtelMetricNameTransformer.transformName("QueueSize"));
    }

    @Test
    public void testSpecialCase_NumWorkers() {
        assertEquals(PREFIX + "workers", OtelMetricNameTransformer.transformName("NumWorkers"));
    }

    @Test
    public void testSpecialCase_NumWorkersWithInvalidEntry() {
        assertEquals(
                PREFIX + "workers.invalid_entry",
                OtelMetricNameTransformer.transformName("NumWorkersWithInvalidEntry"));
    }

    @Test
    public void testSpecialCase_NumWorkersWithFailingWorkerMetric() {
        assertEquals(
                PREFIX + "workers.failing_worker_metric",
                OtelMetricNameTransformer.transformName("NumWorkersWithFailingWorkerMetric"));
    }

    // -----------------------------------------------------------------------
    // Step 2: Colon substitution — ':' replaced with '.'
    // -----------------------------------------------------------------------

    @Test
    public void testColonSubstitution() {
        String result = OtelMetricNameTransformer.transformName("GetLease:Error");
        assertEquals(PREFIX + "get_lease.error", result);
    }

    @Test
    public void testColonSubstitution_multipleColons() {
        String result = OtelMetricNameTransformer.transformName("Foo:Bar:Baz");
        assertEquals(PREFIX + "foo.bar.baz", result);
    }

    // -----------------------------------------------------------------------
    // Step 3: Trailing .Time → .duration
    // -----------------------------------------------------------------------

    @Test
    public void testTrailingTime_RenewLeaseTime() {
        assertEquals(PREFIX + "renew_lease.duration", OtelMetricNameTransformer.transformName("RenewLease.Time"));
    }

    @Test
    public void testTrailingTime_RecordProcessorProcessRecordsTime() {
        assertEquals(
                PREFIX + "record_processor.process_records.duration",
                OtelMetricNameTransformer.transformName("RecordProcessor.processRecords.Time"));
    }

    @Test
    public void testTrailingTime_colonThenTime() {
        // "SomeOp:Time" → colon becomes dot → "SomeOp.Time" → trailing .Time → "SomeOp.duration"
        assertEquals(PREFIX + "some_op.duration", OtelMetricNameTransformer.transformName("SomeOp:Time"));
    }

    @Test
    public void testTimeNotTrailing_isNotReplaced() {
        // "TimeSpent" does not end with ".Time", so no replacement
        String result = OtelMetricNameTransformer.transformName("TimeSpent");
        assertEquals(PREFIX + "time_spent", result);
    }

    // -----------------------------------------------------------------------
    // Step 4: Strip trailing .Count (non-special-case)
    // -----------------------------------------------------------------------

    @Test
    public void testStripTrailingCount() {
        // "SomeMetric.Count" is not in the special-case map, so .Count is stripped
        String result = OtelMetricNameTransformer.transformName("SomeMetric.Count");
        assertEquals(PREFIX + "some_metric", result);
    }

    @Test
    public void testCountNotTrailing_isNotStripped() {
        // "CountOfItems" does not end with ".Count"
        String result = OtelMetricNameTransformer.transformName("CountOfItems");
        assertEquals(PREFIX + "count_of_items", result);
    }

    // -----------------------------------------------------------------------
    // Step 5: PascalCase splitting
    // -----------------------------------------------------------------------

    @Test
    public void testPascalCaseSplitting_RecordsProcessed() {
        assertEquals(PREFIX + "records_processed", OtelMetricNameTransformer.transformName("RecordsProcessed"));
    }

    @Test
    public void testPascalCaseSplitting_LeaseSpillover() {
        assertEquals(PREFIX + "lease_spillover", OtelMetricNameTransformer.transformName("LeaseSpillover"));
    }

    @Test
    public void testPascalCaseSplitting_singleWord() {
        assertEquals(PREFIX + "success", OtelMetricNameTransformer.transformName("Success"));
    }

    @Test
    public void testPascalCaseSplitting_camelCase() {
        assertEquals(PREFIX + "process_records", OtelMetricNameTransformer.transformName("processRecords"));
    }

    @Test
    public void testPascalCaseSplitting_withDotSeparatedSegments() {
        // Each segment is split independently
        String result = OtelMetricNameTransformer.transformName("RecordProcessor.processRecords");
        assertEquals(PREFIX + "record_processor.process_records", result);
    }

    // -----------------------------------------------------------------------
    // Step 5 (continued): Uppercase runs — consecutive uppercase treated as one word
    // -----------------------------------------------------------------------

    @Test
    public void testUppercaseRuns_GSIReadyStatus() {
        // "GSIReadyStatus" → "GSI_Ready_Status" → lowercase → "gsi_ready_status"
        assertEquals(PREFIX + "gsi_ready_status", OtelMetricNameTransformer.transformName("GSIReadyStatus"));
    }

    @Test
    public void testUppercaseRuns_DDBTableName() {
        // "DDBTableName" → "DDB_Table_Name" → lowercase → "ddb_table_name"
        assertEquals(PREFIX + "ddb_table_name", OtelMetricNameTransformer.transformName("DDBTableName"));
    }

    @Test
    public void testUppercaseRuns_allUppercase() {
        // "ABC" → no transitions → "ABC" → lowercase → "abc"
        assertEquals(PREFIX + "abc", OtelMetricNameTransformer.transformName("ABC"));
    }

    // -----------------------------------------------------------------------
    // Step 6: Lowercase — verified implicitly in all tests above
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Step 7: Namespace prefix — all results start with "aws.kinesis.client."
    // -----------------------------------------------------------------------

    @Test
    public void testNamespacePrefix_alwaysPresent() {
        assertTrue(OtelMetricNameTransformer.transformName("AnyMetric").startsWith(PREFIX));
    }

    @Test
    public void testNamespacePrefix_specialCaseAlsoHasPrefix() {
        assertTrue(OtelMetricNameTransformer.transformName("NumWorkers").startsWith(PREFIX));
    }

    // -----------------------------------------------------------------------
    // Step 8: Simple names
    // -----------------------------------------------------------------------

    @Test
    public void testSimpleName_Success() {
        assertEquals(PREFIX + "success", OtelMetricNameTransformer.transformName("Success"));
    }

    @Test
    public void testSimpleName_Time() {
        // "Time" does not end with ".Time" (no dot), so it goes through PascalCase splitting
        // which produces "Time" → lowercase → "time"
        assertEquals(PREFIX + "time", OtelMetricNameTransformer.transformName("Time"));
    }

    // -----------------------------------------------------------------------
    // transformName with unit overload — delegates correctly
    // -----------------------------------------------------------------------

    @Test
    public void testTransformNameWithUnit_delegatesToSingleArg() {
        String withoutUnit = OtelMetricNameTransformer.transformName("RecordsProcessed");
        String withUnit = OtelMetricNameTransformer.transformName("RecordsProcessed", StandardUnit.COUNT);
        assertEquals(withoutUnit, withUnit);
    }

    @Test
    public void testTransformNameWithUnit_nullUnit() {
        String withoutUnit = OtelMetricNameTransformer.transformName("LeaseSpillover");
        String withUnit = OtelMetricNameTransformer.transformName("LeaseSpillover", null);
        assertEquals(withoutUnit, withUnit);
    }

    @Test
    public void testTransformNameWithUnit_specialCase() {
        String withoutUnit = OtelMetricNameTransformer.transformName("MillisBehindLatest");
        String withUnit = OtelMetricNameTransformer.transformName("MillisBehindLatest", StandardUnit.MILLISECONDS);
        assertEquals(withoutUnit, withUnit);
    }

    @Test
    public void testTransformNameWithUnit_variousUnits() {
        String name = "DataBytesProcessed";
        String expected = OtelMetricNameTransformer.transformName(name);
        assertEquals(expected, OtelMetricNameTransformer.transformName(name, StandardUnit.BYTES));
        assertEquals(expected, OtelMetricNameTransformer.transformName(name, StandardUnit.NONE));
        assertEquals(expected, OtelMetricNameTransformer.transformName(name, StandardUnit.MILLISECONDS));
    }

    // -----------------------------------------------------------------------
    // Edge cases and validation
    // -----------------------------------------------------------------------

    @Test
    public void testEmptySegmentPreserved() {
        // "Foo..Bar" has an empty segment between dots
        String result = OtelMetricNameTransformer.transformName("Foo..Bar");
        assertEquals(PREFIX + "foo..bar", result);
    }

    @Test
    public void testSingleCharacterName() {
        assertEquals(PREFIX + "x", OtelMetricNameTransformer.transformName("X"));
    }

    @Test
    public void testAlreadyLowercase() {
        assertEquals(PREFIX + "already_lower", OtelMetricNameTransformer.transformName("alreadyLower"));
    }

    @Test
    public void testNumericInName() {
        // "Retry3Count" — numeric characters don't trigger splits
        String result = OtelMetricNameTransformer.transformName("Retry3Count");
        // "Retry3" stays together, "Count" splits → "Retry3_Count" → lowercase
        assertEquals(PREFIX + "retry3_count", result);
    }
}
