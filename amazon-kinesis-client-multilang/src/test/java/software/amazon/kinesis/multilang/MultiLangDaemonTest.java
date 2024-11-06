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
package software.amazon.kinesis.multilang;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MultiLangDaemonTest {
    @Mock
    private Scheduler scheduler;

    @Mock
    private MultiLangDaemonConfig config;

    @Mock
    private ExecutorService executorService;

    @Mock
    private Future<Integer> futureInteger;

    @Mock
    private MultiLangDaemonConfiguration multiLangDaemonConfiguration;

    @Mock
    private Runtime runtime;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private MultiLangDaemon daemon;

    @Before
    public void setup() {
        daemon = new MultiLangDaemon() {
            @Override
            Scheduler buildScheduler(final MultiLangDaemonConfig configuration) {
                return scheduler;
            }
        };
    }

    @Test
    public void testSuccessfulNoOptionsJCommanderBuild() {
        String testPropertiesFile = "/test/properties/file";
        MultiLangDaemon.MultiLangDaemonArguments arguments = new MultiLangDaemon.MultiLangDaemonArguments();
        daemon.buildJCommanderAndParseArgs(arguments, new String[] {testPropertiesFile});

        assertThat(arguments.propertiesFile, nullValue());
        assertThat(arguments.logConfiguration, nullValue());
        assertThat(arguments.parameters.size(), equalTo(1));
        assertThat(arguments.parameters.get(0), equalTo(testPropertiesFile));
    }

    @Test
    public void testSuccessfulOptionsJCommanderBuild() {
        String propertiesOption = "/test/properties/file/option";
        String propertiesFileArgs = "/test/properties/args";
        String[] args = new String[] {"-p", propertiesOption, propertiesFileArgs};
        MultiLangDaemon.MultiLangDaemonArguments arguments = new MultiLangDaemon.MultiLangDaemonArguments();
        daemon.buildJCommanderAndParseArgs(arguments, args);

        assertThat(arguments.propertiesFile, equalTo(propertiesOption));
        assertThat(arguments.logConfiguration, nullValue());
        assertThat(arguments.parameters.size(), equalTo(1));
        assertThat(arguments.parameters.get(0), equalTo(propertiesFileArgs));
    }

    @Test
    public void testEmptyArgsJCommanderBuild() {
        MultiLangDaemon.MultiLangDaemonArguments arguments = new MultiLangDaemon.MultiLangDaemonArguments();
        String[] args = new String[] {};
        daemon.buildJCommanderAndParseArgs(arguments, args);

        assertThat(arguments.propertiesFile, nullValue());
        assertThat(arguments.logConfiguration, nullValue());
        assertThat(arguments.parameters, empty());
    }

    @Test
    public void testSuccessfulLoggingConfiguration() {
        LoggerContext loggerContext = spy((LoggerContext) LoggerFactory.getILoggerFactory());
        JoranConfigurator configurator = spy(new JoranConfigurator());

        String logConfiguration =
                this.getClass().getClassLoader().getResource("logback.xml").getPath();
        daemon.configureLogging(logConfiguration, loggerContext, configurator);

        verify(loggerContext).reset();
        verify(configurator).setContext(eq(loggerContext));
    }

    @Test
    public void testUnsuccessfulLoggingConfiguration() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(containsString("Error while loading log configuration:"));

        String logConfiguration = "blahblahblah";

        daemon.configureLogging(logConfiguration, loggerContext, configurator);
    }

    @Test
    public void testNoPropertiesFileArgumentOrOption() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(equalTo("Properties file missing, please provide a properties file"));

        MultiLangDaemon.MultiLangDaemonArguments arguments = new MultiLangDaemon.MultiLangDaemonArguments();

        daemon.validateAndGetPropertiesFileName(arguments);
    }

    @Test
    public void testSuccessfulPropertiesArgument() {
        String expectedPropertiesFile = "/test/properties/file";
        MultiLangDaemon.MultiLangDaemonArguments arguments = new MultiLangDaemon.MultiLangDaemonArguments();
        arguments.parameters = Collections.singletonList(expectedPropertiesFile);

        String propertiesFile = daemon.validateAndGetPropertiesFileName(arguments);

        assertThat(propertiesFile, equalTo(expectedPropertiesFile));
    }

    @Test
    public void testPropertiesOptionsOverrideArgument() {
        String propertiesArgument = "/test/properties/argument";
        String propertiesOptions = "/test/properties/options";

        MultiLangDaemon.MultiLangDaemonArguments arguments = new MultiLangDaemon.MultiLangDaemonArguments();
        arguments.parameters = Collections.singletonList(propertiesArgument);
        arguments.propertiesFile = propertiesOptions;

        String propertiesFile = daemon.validateAndGetPropertiesFileName(arguments);

        assertThat(propertiesFile, equalTo(propertiesOptions));
    }

    @Test
    public void testExtraArgumentsFailure() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(containsString("Expected a single argument, but found multiple arguments."));

        MultiLangDaemon.MultiLangDaemonArguments arguments = new MultiLangDaemon.MultiLangDaemonArguments();
        arguments.parameters = Arrays.asList("parameter1", "parameter2");

        daemon.validateAndGetPropertiesFileName(arguments);
    }

    @Test
    public void testBuildMultiLangConfigMissingPropertiesFile() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(containsString("Error while reading properties file:"));

        daemon.buildMultiLangDaemonConfig("blahblahblah");
    }

    @Test
    public void testBuildMultiLangConfigWithIncorrectInformation() throws IOException {
        File propertiesFile = temporaryFolder.newFile("temp.properties");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(containsString("Must provide an executable name in the properties file"));

        daemon.buildMultiLangDaemonConfig(propertiesFile.getAbsolutePath());
    }

    @Test
    public void testSuccessfulSubmitRunnerAndWait() throws Exception {
        int expectedExitCode = 0;

        MultiLangDaemon.MultiLangRunner runner = new MultiLangDaemon.MultiLangRunner(scheduler);
        when(config.getExecutorService()).thenReturn(executorService);
        when(executorService.submit(eq(runner))).thenReturn(futureInteger);
        when(futureInteger.get()).thenReturn(expectedExitCode);

        int exitCode = daemon.submitRunnerAndWait(config, runner);

        assertThat(exitCode, equalTo(expectedExitCode));
    }

    @Test
    public void testErrorSubmitRunnerAndWait() throws Exception {
        int expectedExitCode = 1;

        MultiLangDaemon.MultiLangRunner runner = new MultiLangDaemon.MultiLangRunner(scheduler);
        when(config.getExecutorService()).thenReturn(executorService);
        when(executorService.submit(eq(runner))).thenReturn(futureInteger);
        when(futureInteger.get()).thenThrow(ExecutionException.class);

        int exitCode = daemon.submitRunnerAndWait(config, runner);

        assertThat(exitCode, equalTo(expectedExitCode));
    }

    @Test
    public void testSetupShutdownHook() {
        when(config.getMultiLangDaemonConfiguration()).thenReturn(multiLangDaemonConfiguration);
        when(multiLangDaemonConfiguration.getShutdownGraceMillis()).thenReturn(1000L);
        doNothing().when(runtime).addShutdownHook(anyObject());

        MultiLangDaemon.MultiLangRunner runner = new MultiLangDaemon.MultiLangRunner(scheduler);
        daemon.setupShutdownHook(runtime, runner, config);

        verify(multiLangDaemonConfiguration).getShutdownGraceMillis();
        verify(runtime).addShutdownHook(anyObject());
    }

    @Test
    public void testSuccessfulRunner() throws Exception {
        MultiLangDaemon.MultiLangRunner runner = new MultiLangDaemon.MultiLangRunner(scheduler);
        doNothing().when(scheduler).run();

        int exit = runner.call();

        assertThat(exit, equalTo(0));

        verify(scheduler).run();
    }

    @Test
    public void testUnsuccessfulRunner() throws Exception {
        MultiLangDaemon.MultiLangRunner runner = new MultiLangDaemon.MultiLangRunner(scheduler);
        doThrow(Exception.class).when(scheduler).run();

        int exit = runner.call();

        assertThat(exit, equalTo(1));

        verify(scheduler).run();
    }
}
