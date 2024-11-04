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

import java.util.Optional;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ConfigurationSettableUtilsTest {

    @Test
    public void testNoPropertiesSet() {
        ConfigResult expected = ConfigResult.builder().build();

        ConfigObject configObject = ConfigObject.builder().build();
        ConfigResult actual = resolve(configObject);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testPrimitivesSet() {
        ConfigResult expected = ConfigResult.builder().rawInt(10).rawLong(15L).build();

        ConfigObject configObject = ConfigObject.builder()
                .rawInt(expected.rawInt)
                .rawLong(expected.rawLong)
                .build();
        ConfigResult actual = resolve(configObject);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testBoolean() {
        ConfigResult expected = ConfigResult.builder().bool(false).build();

        ConfigObject configObject = ConfigObject.builder().bool(expected.bool).build();
        ConfigResult actual = resolve(configObject);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testHeapValuesSet() {
        ConfigResult expected =
                ConfigResult.builder().name("test").boxedInt(10).boxedLong(15L).build();

        ConfigObject configObject = ConfigObject.builder()
                .name(expected.name)
                .boxedInt(expected.boxedInt.intValue())
                .boxedLong(expected.boxedLong.longValue())
                .build();
        ConfigResult actual = resolve(configObject);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexValuesSet() {
        ComplexValue complexValue =
                ComplexValue.builder().name("complex").value(10).build();
        ConfigResult expected =
                ConfigResult.builder().complexValue(complexValue).build();

        ConfigObject configObject = ConfigObject.builder()
                .complexValue(ComplexValue.builder()
                        .name(complexValue.name)
                        .value(complexValue.value)
                        .build())
                .build();
        ConfigResult actual = resolve(configObject);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testOptionalValuesSet() {
        ComplexValue complexValue =
                ComplexValue.builder().name("optional-complex").value(20).build();
        ConfigResult expected = ConfigResult.builder()
                .optionalString(Optional.of("test"))
                .optionalInteger(Optional.of(10))
                .optionalLong(Optional.of(15L))
                .optionalComplexValue(Optional.of(complexValue))
                .build();

        ConfigObject configObject = ConfigObject.builder()
                .optionalString(expected.optionalString.get())
                .optionalInteger(expected.optionalInteger.get())
                .optionalLong(expected.optionalLong.get())
                .optionalComplexValue(expected.optionalComplexValue.get())
                .build();
        ConfigResult actual = resolve(configObject);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testRenamedRawValues() {
        ComplexValue complexValue =
                ComplexValue.builder().name("renamed-complex").value(20).build();
        ConfigResult expected = ConfigResult.builder()
                .renamedString("renamed")
                .renamedInt(10)
                .renamedOptionalString(Optional.of("renamed-optional"))
                .renamedComplexValue(complexValue)
                .build();

        ConfigObject configObject = ConfigObject.builder()
                .toRenameString(expected.renamedString)
                .toRenameInt(expected.renamedInt)
                .toRenameComplexValue(complexValue)
                .optionalToRename(expected.renamedOptionalString.get())
                .build();
        ConfigResult actual = resolve(configObject);

        assertThat(actual, equalTo(expected));
    }

    private ConfigResult resolve(ConfigObject configObject) {
        return ConfigurationSettableUtils.resolveFields(
                configObject, ConfigResult.builder().build());
    }

    @Accessors(fluent = true)
    @Builder
    @Getter
    @Setter
    @EqualsAndHashCode
    public static class ConfigResult {
        private String name;
        private int rawInt;
        private Integer boxedInt;
        private long rawLong;
        private Long boxedLong;
        private ComplexValue complexValue;

        @Builder.Default
        private Boolean bool = true;

        private Optional<String> optionalString;
        private Optional<Integer> optionalInteger;
        private Optional<Long> optionalLong;
        private Optional<ComplexValue> optionalComplexValue;

        private String renamedString;
        private int renamedInt;
        private Optional<String> renamedOptionalString;
        private ComplexValue renamedComplexValue;
    }

    @Accessors(fluent = true)
    @Builder
    @EqualsAndHashCode
    public static class ComplexValue {
        private String name;
        private int value;
    }

    @Builder
    public static class ConfigObject {

        @ConfigurationSettable(configurationClass = ConfigResult.class)
        private String name;

        @ConfigurationSettable(configurationClass = ConfigResult.class)
        private int rawInt;

        @ConfigurationSettable(configurationClass = ConfigResult.class)
        @Builder.Default
        private Boolean bool = true;

        @ConfigurationSettable(configurationClass = ConfigResult.class)
        private Integer boxedInt;

        @ConfigurationSettable(configurationClass = ConfigResult.class)
        private long rawLong;

        @ConfigurationSettable(configurationClass = ConfigResult.class)
        private Long boxedLong;

        @ConfigurationSettable(configurationClass = ConfigResult.class)
        private ComplexValue complexValue;

        @ConfigurationSettable(configurationClass = ConfigResult.class, convertToOptional = true)
        private String optionalString;

        @ConfigurationSettable(configurationClass = ConfigResult.class, convertToOptional = true)
        private Integer optionalInteger;

        @ConfigurationSettable(configurationClass = ConfigResult.class, convertToOptional = true)
        private Long optionalLong;

        @ConfigurationSettable(configurationClass = ConfigResult.class, convertToOptional = true)
        private ComplexValue optionalComplexValue;

        @ConfigurationSettable(configurationClass = ConfigResult.class, methodName = "renamedString")
        private String toRenameString;

        @ConfigurationSettable(configurationClass = ConfigResult.class, methodName = "renamedInt")
        private int toRenameInt;

        @ConfigurationSettable(
                configurationClass = ConfigResult.class,
                methodName = "renamedOptionalString",
                convertToOptional = true)
        private String optionalToRename;

        @ConfigurationSettable(configurationClass = ConfigResult.class, methodName = "renamedComplexValue")
        private ComplexValue toRenameComplexValue;
    }
}
