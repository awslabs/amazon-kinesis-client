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

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

@Getter
@ToString(exclude = "MESSAGE")
@Slf4j
@KinesisClientInternalApi
public class RejectedTaskEvent implements DiagnosticEvent {
    private final String MESSAGE = "Unexpected task rejection occurred. This could possibly " +
            "be an issue or a bug. Please search for the exception/error online to check what is " +
            "going on. If the issue persists or is a recurring problem, feel free to open an issue " +
            "at https://github.com/awslabs/amazon-kinesis-client. ";

    private ExecutorStateEvent executorStateEvent;
    private Throwable throwable;

    public RejectedTaskEvent(ExecutorStateEvent executorStateEvent, Throwable throwable) {
        this.executorStateEvent = executorStateEvent;
        this.throwable = throwable;
    }

    @Override
    public void accept(DiagnosticEventHandler visitor) { visitor.visit(this); }

    @Override
    public String message() {
        return MESSAGE + executorStateEvent.message();
    }
}
