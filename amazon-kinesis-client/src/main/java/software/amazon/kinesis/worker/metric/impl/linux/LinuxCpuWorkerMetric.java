/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.worker.metric.impl.linux;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

/**
 * Reads CPU usage statistics out of /proc/stat file that is present on the EC2 instances. The value is % utilization
 * of the CPU.
 * When this is invoked for the first time, the value returned is always 0 as the prev values are not available
 * to calculate the diff. If the file hasn't changed this also returns 0.
 * In case the file is not present or any other exception occurs, this throws IllegalArgumentException.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@KinesisClientInternalApi
public class LinuxCpuWorkerMetric implements WorkerMetric {

    private static final Object LOCK_OBJECT = new Object();
    private static final WorkerMetricType CPU_WORKER_METRICS_TYPE = WorkerMetricType.CPU;
    private final OperatingRange operatingRange;
    private final String statFile;
    private long lastUsr, lastIow, lastSys, lastIdl, lastTot;
    private String lastLine;

    public LinuxCpuWorkerMetric(final OperatingRange operatingRange) {
        this(operatingRange, "/proc/stat");
    }

    @Override
    public String getShortName() {
        return CPU_WORKER_METRICS_TYPE.getShortName();
    }

    @Override
    public WorkerMetricValue capture() {
        return WorkerMetricValue.builder().value(calculateCpuUsage()).build();
    }

    private double calculateCpuUsage() {
        BufferedReader bufferedReader = null;
        try {

            final File stat = new File(statFile);
            if (stat.exists()) {

                bufferedReader = new BufferedReader(new FileReader(stat));
                final String line = bufferedReader.readLine();
                final String[] lineVals = line.split("\\s+");

                long usr = Long.parseLong(lineVals[1]) + Long.parseLong(lineVals[2]);
                long sys = Long.parseLong(lineVals[3]);
                long idl = Long.parseLong(lineVals[4]);
                long iow = Long.parseLong(lineVals[5]);
                long tot = usr + sys + idl + iow;
                long diffIdl = -1;
                long diffTot = -1;

                boolean skip = false;
                synchronized (LOCK_OBJECT) {
                    if (lastUsr == 0 || line.equals(lastLine)) {
                        // Case where this is a first call so no diff available or
                        // /proc/stat file is not updated since last time.
                        skip = true;
                    }

                    diffIdl = Math.abs(idl - lastIdl);
                    diffTot = Math.abs(tot - lastTot);
                    if (diffTot < diffIdl) {
                        log.warn(
                                "diffTot is less than diff_idle. \nPrev cpu line : {} and current cpu line : {} ",
                                lastLine,
                                line);
                        if (iow < lastIow) {
                            // this is case where current iow value less than prev, this can happen in rare cases as per
                            // https://docs.kernel.org/filesystems/proc.html, and when the worker is idle
                            // there is no increase in usr or sys values as well resulting in diffTot < diffIdl as
                            // current tot increases less than current idl
                            // return 0 in this case as this is the case where worker is not doing anything anyways.
                            skip = true;
                        }
                    }
                    lastUsr = usr;
                    lastSys = sys;
                    lastIdl = idl;
                    lastIow = iow;
                    lastTot = usr + sys + idl + iow;
                    lastLine = line;
                }

                if (skip) {
                    return 0D;
                }

                return ((double) (diffTot - diffIdl) / (double) diffTot) * 100.0;

            } else {
                throw new IllegalArgumentException(String.format(
                        "LinuxCpuWorkerMetric is not configured properly, file : %s does not exists", this.statFile));
            }
        } catch (final Throwable t) {
            if (t instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) t;
            }
            throw new IllegalArgumentException(
                    "LinuxCpuWorkerMetric failed to read metric stats or not configured properly.", t);
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (Throwable x) {
                log.warn("Failed to close bufferedReader ", x);
            }
        }
    }

    @Override
    public OperatingRange getOperatingRange() {
        return operatingRange;
    }

    @Override
    public WorkerMetricType getWorkerMetricType() {
        return CPU_WORKER_METRICS_TYPE;
    }
}
