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

/**
 * An interface to implement various types of stateful events that can be used for diagnostics throughout the KCL.
 */
interface DiagnosticEvent {
    /**
     * DiagnosticEvent is part of a visitor pattern and it accepts DiagnosticEventHandler visitors.
     *
     * @param visitor A handler that that controls the behavior of the DiagnosticEvent when invoked.
     */
    void accept(DiagnosticEventHandler visitor);

    /**
     * The string to output to logs when a DiagnosticEvent occurs.
     */
    String message();
}
