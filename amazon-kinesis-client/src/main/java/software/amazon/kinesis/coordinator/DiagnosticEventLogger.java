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
package software.amazon.kinesis.coordinator;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Internal implementation of {@link DiagnosticEventHandler} used by {@link Scheduler} to log executor state both
 * 1) in normal conditions periodically, and 2) in reaction to rejected task exceptions.
 */
@NoArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class DiagnosticEventLogger implements DiagnosticEventHandler {
    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(ExecutorStateEvent event) {
        log.info(event.message());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(RejectedTaskEvent event) {
        log.error(event.message(), event.getThrowable());
    }
}
