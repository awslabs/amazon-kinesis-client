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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * KinesisClientLibConfigurator constructs a KinesisClientLibConfiguration from java properties file. The following
 * three properties must be provided. 1) "applicationName" 2) "streamName" 3) "AWSCredentialsProvider"
 * KinesisClientLibConfigurator will help to automatically assign the value of "workerId" if this property is not
 * provided. In the specified properties file, any properties, which matches the variable name in
 * KinesisClientLibConfiguration and has a corresponding "with{variableName}" setter method, will be read in, and its
 * value in properties file will be assigned to corresponding variable in KinesisClientLibConfiguration.
 */
@Slf4j
public class KinesisClientLibConfigurator {
    private final ConvertUtilsBean convertUtilsBean;
    private final BeanUtilsBean utilsBean;
    private final MultiLangDaemonConfiguration configuration;


    /**
     * Constructor.
     */
    public KinesisClientLibConfigurator() {
        this.convertUtilsBean = new ConvertUtilsBean();
        this.utilsBean = new BeanUtilsBean(convertUtilsBean);
        this.configuration = new MultiLangDaemonConfiguration(utilsBean, convertUtilsBean);
    }

    /**
     * Return a KinesisClientLibConfiguration with variables configured as specified by the properties in config stream.
     * Program will fail immediately, if customer provide: 1) invalid variable value. Program will log it as warning and
     * continue, if customer provide: 1) variable with unsupported variable type. 2) a variable with name which does not
     * match any of the variables in KinesisClientLibConfigration.
     * 
     * @param properties a Properties object containing the configuration information
     * @return KinesisClientLibConfiguration
     */
    public MultiLangDaemonConfiguration getConfiguration(Properties properties) {
        properties.entrySet().forEach(e -> {
            try {
                utilsBean.setProperty(configuration, (String) e.getKey(), e.getValue());
            } catch (IllegalAccessException | InvocationTargetException ex) {
                throw new RuntimeException(ex);
            }
        });

        Validate.notBlank(configuration.getApplicationName(), "Application name is required");
        Validate.notBlank(configuration.getStreamName(), "Stream name is required");
        Validate.isTrue(configuration.getKinesisCredentialsProvider().isDirty(), "A basic set of AWS credentials must be provided");
        return configuration;
    }

    /**
     * @param configStream the input stream containing the configuration information
     * @return KinesisClientLibConfiguration
     */
    public MultiLangDaemonConfiguration getConfiguration(InputStream configStream) {
        Properties properties = new Properties();
        try {
            properties.load(configStream);
        } catch (IOException e) {
            String msg = "Could not load properties from the stream provided";
            throw new IllegalStateException(msg, e);
        } finally {
            try {
                configStream.close();
            } catch (IOException e) {
                String msg = "Encountered error while trying to close properties file.";
                throw new IllegalStateException(msg, e);
            }
        }
        return getConfiguration(properties);
    }


}
