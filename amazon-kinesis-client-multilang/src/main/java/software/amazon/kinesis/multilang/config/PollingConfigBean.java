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

package software.amazon.kinesis.multilang.config;

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

@Getter
@Setter
public class PollingConfigBean implements RetrievalConfigBuilder {

    /**
     * This is used to auto-generate a delegate by Lombok at {@link MultiLangDaemonConfiguration#getPollingConfig()}
     */
    interface PollingConfigBeanDelegate {

        Integer getRetryGetRecordsInSeconds();
        void setRetryGetRecordsInSeconds(Integer value);

        Integer getMaxGetRecordsThreadPool();
        void setMaxGetRecordsThreadPool(Integer value);

        long getIdleTimeBetweenReadsInMillis();
        void setIdleTimeBetweenReadsInMillis(long value);

        int getMaxRecords();
        void setMaxRecords(int value);
    }

    @ConfigurationSettable(configurationClass = PollingConfig.class, convertToOptional = true)
    private Integer retryGetRecordsInSeconds;
    @ConfigurationSettable(configurationClass = PollingConfig.class, convertToOptional = true)
    private Integer maxGetRecordsThreadPool;
    @ConfigurationSettable(configurationClass = PollingConfig.class)
    private long idleTimeBetweenReadsInMillis;
    @ConfigurationSettable(configurationClass = PollingConfig.class)
    private int maxRecords;

    public boolean anyPropertiesSet() {
        return retryGetRecordsInSeconds != null || maxGetRecordsThreadPool != null || idleTimeBetweenReadsInMillis != 0 || maxRecords != 0;
    }

    @Override
    public PollingConfig build(KinesisAsyncClient kinesisAsyncClient, MultiLangDaemonConfiguration parent) {
        return ConfigurationSettableUtils.resolveFields(this, new PollingConfig(parent.getStreamName(), kinesisAsyncClient));
    }

}
