/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.leases.exceptions;

/**
 * Top-level exception type for all exceptions thrown by the leasing code.
 */
public class LeasingException extends Exception {

    public LeasingException(Throwable e) {
        super(e);
    }

    public LeasingException(String message, Throwable e) {
        super(message, e);
    }

    public LeasingException(String message) {
        super(message);
    }

    private static final long serialVersionUID = 1L;

}
