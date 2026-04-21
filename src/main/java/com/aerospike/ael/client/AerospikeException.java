/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.ael.client;

import java.util.List;

/**
 * Aerospike exceptions that can be thrown from the client.
 */
public class AerospikeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    protected List<AerospikeException> subExceptions;
    protected int resultCode = ResultCode.CLIENT_ERROR;
    protected int iteration = -1;
    protected boolean inDoubt;

    public AerospikeException(int resultCode, String message) {
        super(message);
        this.resultCode = resultCode;
    }

    public AerospikeException(int resultCode, Throwable e) {
        super(e);
        this.resultCode = resultCode;
    }

    public AerospikeException(int resultCode) {
        super();
        this.resultCode = resultCode;
    }

    public AerospikeException(int resultCode, boolean inDoubt) {
        super();
        this.resultCode = resultCode;
        this.inDoubt = inDoubt;
    }

    public AerospikeException(int resultCode, String message, Throwable e) {
        super(message, e);
        this.resultCode = resultCode;
    }

    public AerospikeException(String message, Throwable e) {
        super(message, e);
    }

    public AerospikeException(String message) {
        super(message);
    }

    public AerospikeException(Throwable e) {
        super(e);
    }

    /**
     * Exception thrown when a Java serialization error occurs.
     */
    public static final class Serialize extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public Serialize(Throwable e) {
            super(ResultCode.SERIALIZE_ERROR, "Serialize error", e);
        }

        public Serialize(String message) {
            super(ResultCode.SERIALIZE_ERROR, message);
        }
    }

    /**
     * Exception thrown when client can't parse data returned from server.
     */
    public static final class Parse extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public Parse(String message) {
            super(ResultCode.PARSE_ERROR, message);
        }
    }

    /**
     * Return base message without extra metadata.
     */
    public String getBaseMessage() {
        String message = super.getMessage();
        return (message != null)? message : ResultCode.getResultString(resultCode);
    }

    /**
     * Should connection be put back into pool.
     */
    public final boolean keepConnection() {
        return ResultCode.keepConnection(resultCode);
    }

    /**
     * Get sub exceptions.  Will be null if a retry did not occur.
     */
    public final List<AerospikeException> getSubExceptions() {
        return subExceptions;
    }

    /**
     * Set sub exceptions.
     */
    public final void setSubExceptions(List<AerospikeException> subExceptions) {
        this.subExceptions = subExceptions;
    }

    /**
     * Get integer result code.
     */
    public final int getResultCode() {
        return resultCode;
    }

    /**
     * Get number of attempts before failing.
     */
    public final int getIteration() {
        return iteration;
    }

    /**
     * Set number of attempts before failing.
     */
    public final void setIteration(int iteration) {
        this.iteration = iteration;
    }

    /**
     * Is it possible that write command may have completed.
     */
    public final boolean getInDoubt() {
        return inDoubt;
    }

    /**
     * Set whether it is possible that the write command may have completed
     * even though this exception was generated.  This may be the case when a
     * client error occurs (like timeout) after the command was sent to the server.
     */
    public final void setInDoubt(boolean isWrite, int commandSentCounter) {
        if (isWrite && (commandSentCounter > 1 || (commandSentCounter == 1 && (resultCode == ResultCode.TIMEOUT || resultCode <= 0)))) {
            this.inDoubt = true;
        }
    }

    /**
     * Sets the inDoubt value to inDoubt.
     */
    public void setInDoubt(boolean inDoubt) {
        this.inDoubt = inDoubt;
    }
}
