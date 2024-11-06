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

package software.amazon.kinesis.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

@Slf4j
@KinesisClientInternalApi
public class Cgroup {

    public static String readSingleLineFile(String path) {
        BufferedReader bufferedReader = null;
        try {
            final File file = new File(path);
            if (file.exists()) {
                bufferedReader = new BufferedReader(new FileReader(file));
                return bufferedReader.readLine();
            } else {
                throw new IllegalArgumentException(String.format("Failed to read file. %s does not exist", path));
            }
        } catch (final Throwable t) {
            if (t instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) t;
            }
            throw new IllegalArgumentException("Failed to read file.", t);
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

    /**
     * Calculates the number of available cpus from the cpuset
     * See https://docs.kernel.org/admin-guide/cgroup-v2.html#cpuset for more information
     * "0-7" represents 8 cores
     * "0-4,6,8-10" represents 9 cores (cores 0,1,2,3,4 and core 6 and core 8,9,10)
     * @param cpuSet a single line from the cgroup cpuset file
     * @return the number of available cpus
     */
    public static int getAvailableCpusFromEffectiveCpuSet(final String cpuSet) {
        final String[] cpuSetArr = cpuSet.split(",");

        int sumCpus = 0;
        for (String cpuSetGroup : cpuSetArr) {
            if (cpuSetGroup.contains("-")) {
                final String[] cpuSetGroupSplit = cpuSetGroup.split("-");
                // Values are inclusive
                sumCpus += Integer.parseInt(cpuSetGroupSplit[1]) - Integer.parseInt(cpuSetGroupSplit[0]) + 1;
            } else {
                sumCpus += 1;
            }
        }
        return sumCpus;
    }
}
