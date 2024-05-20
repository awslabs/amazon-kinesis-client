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
package software.amazon.kinesis.common;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FutureUtilsTest {

    @Mock
    private Future<String> future;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testTimeoutExceptionCancelsFuture() throws Exception {
        expectedException.expect(TimeoutException.class);

        when(future.get(anyLong(), any())).thenThrow(new TimeoutException("Timeout"));

        try {
            FutureUtils.resolveOrCancelFuture(future, Duration.ofSeconds(1));
        } finally {
            verify(future).cancel(eq(true));
        }
    }
}
