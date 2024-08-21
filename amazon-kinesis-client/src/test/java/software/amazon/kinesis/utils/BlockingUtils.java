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

package software.amazon.kinesis.utils;

import java.util.function.Supplier;

public class BlockingUtils {

    public static <Records> Records blockUntilRecordsAvailable(Supplier<Records> recordsSupplier, long timeoutMillis) {
        Records recordsRetrieved;
        while ((recordsRetrieved = recordsSupplier.get()) == null && timeoutMillis > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            timeoutMillis -= 100;
        }
        if (recordsRetrieved != null) {
            return recordsRetrieved;
        } else {
            throw new RuntimeException("No records found");
        }
    }

    public static boolean blockUntilConditionSatisfied(Supplier<Boolean> conditionSupplier, long timeoutMillis) {
        while (!conditionSupplier.get() && timeoutMillis > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            timeoutMillis -= 100;
        }
        return conditionSupplier.get();
    }
}
