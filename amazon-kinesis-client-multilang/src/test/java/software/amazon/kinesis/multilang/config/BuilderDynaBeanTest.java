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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

public class BuilderDynaBeanTest {

    private static boolean isBad = true;
    private ConvertUtilsBean convertUtilsBean;
    private BeanUtilsBean utilsBean;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        convertUtilsBean = new ConvertUtilsBean();
        utilsBean = new BeanUtilsBean(convertUtilsBean);
    }

    @Test
    public void testSimpleCreateAllParameters() throws Exception {
        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleCreate.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "[0]", "first");
        utilsBean.setProperty(builderDynaBean, "[1]", "last");

        TestSimpleCreate expected = TestSimpleCreate.create("first", "last");
        TestSimpleCreate actual = builderDynaBean.build(TestSimpleCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSimpleCreateToManyParameters() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("exceeds the maximum"));

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleCreate.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "[0]", "first");
        utilsBean.setProperty(builderDynaBean, "[1]", "last");
        utilsBean.setProperty(builderDynaBean, "[2]", "age");

        TestSimpleCreate expected = TestSimpleCreate.create("first", "last");
        TestSimpleCreate actual = builderDynaBean.build(TestSimpleCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSimpleCreateMissingParameter() throws Exception {
        TestSimpleCreate expected = TestSimpleCreate.create(null, "last");

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleCreate.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[1]", expected.lastName);

        TestSimpleCreate actual = builderDynaBean.build(TestSimpleCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSimpleCreateNoParameters() throws Exception {
        TestSimpleCreate expected = TestSimpleCreate.create(null, null);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleCreate.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[1]", expected.lastName);

        TestSimpleCreate actual = builderDynaBean.build(TestSimpleCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateAllParameters() throws Exception {
        TestComplexCreate expected = TestComplexCreate.create("real",
                TestSimpleBuilder.builder().stringL1("l1").longVal(10L).build());

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreate.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.realName);
        utilsBean.setProperty(builderDynaBean, "[1].stringL1", expected.test1.stringL1);
        utilsBean.setProperty(builderDynaBean, "[1].longVal", expected.test1.longVal);

        TestComplexCreate actual = builderDynaBean.build(TestComplexCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateSimpleParameterOnly() throws Exception {
        TestComplexCreate expected = TestComplexCreate.create("real", null);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreate.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.realName);

        TestComplexCreate actual = builderDynaBean.build(TestComplexCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateComplexParameterOnly() throws Exception {
        TestComplexCreate expected = TestComplexCreate.create(null,
                TestSimpleBuilder.builder().stringL1("l1").longVal(10L).build());

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreate.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[1].stringL1", expected.test1.stringL1);
        utilsBean.setProperty(builderDynaBean, "[1].longVal", expected.test1.longVal);

        TestComplexCreate actual = builderDynaBean.build(TestComplexCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateNoParameters() throws Exception {
        TestComplexCreate expected = TestComplexCreate.create(null, null);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreate.class, convertUtilsBean);

        TestComplexCreate actual = builderDynaBean.build(TestComplexCreate.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSimpleBuilderAllParameters() throws Exception {
        TestSimpleBuilder expected = TestSimpleBuilder.builder().stringL1("l1").longVal(10L).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "stringL1", expected.stringL1);
        utilsBean.setProperty(builderDynaBean, "longVal", expected.longVal);

        TestSimpleBuilder actual = builderDynaBean.build(TestSimpleBuilder.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSimpleBuilderMissingStringL1() throws Exception {
        TestSimpleBuilder expected = TestSimpleBuilder.builder().longVal(10L).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "longVal", expected.longVal);

        TestSimpleBuilder actual = builderDynaBean.build(TestSimpleBuilder.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSimpleBuilderMissingLongVal() throws Exception {
        TestSimpleBuilder expected = TestSimpleBuilder.builder().stringL1("l1").build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "stringL1", expected.stringL1);

        TestSimpleBuilder actual = builderDynaBean.build(TestSimpleBuilder.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSimpleBuilderInvalidProperty() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unknown property: invalidProperty");

        TestSimpleBuilder expected = TestSimpleBuilder.builder().build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "invalidProperty", "invalid");

        TestSimpleBuilder actual = builderDynaBean.build(TestSimpleBuilder.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateSimpleBuilderVariantAllParameters() throws Exception {
        TestSimpleBuilder variant = TestSimpleBuilder.builder().longVal(10L).stringL1("variant").build();
        TestComplexCreateVariance expected = TestComplexCreateVariance.create("simple-builder", variant);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreateVariance.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.varianceName);
        utilsBean.setProperty(builderDynaBean, "[1].class", expected.variant.getClass().getName());
        utilsBean.setProperty(builderDynaBean, "[1].longVal", variant.longVal);
        utilsBean.setProperty(builderDynaBean, "[1].stringL1", variant.stringL1);

        TestComplexCreateVariance actual = builderDynaBean.build(TestComplexCreateVariance.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateVariantBuilderAllParameters() throws Exception {
        TestVariantBuilder variant = TestVariantBuilder.builder().variantBuilderName("variant-build").intClass(20)
                .testEnum(TestEnum.Blue).build();
        TestComplexCreateVariance expected = TestComplexCreateVariance.create("builder-variant", variant);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreateVariance.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.varianceName);
        utilsBean.setProperty(builderDynaBean, "[1].class", variant.getClass().getName());
        utilsBean.setProperty(builderDynaBean, "[1].variantBuilderName", variant.variantBuilderName);
        utilsBean.setProperty(builderDynaBean, "[1].intClass", variant.intClass);
        utilsBean.setProperty(builderDynaBean, "[1].testEnum", variant.testEnum);

        TestComplexCreateVariance actual = builderDynaBean.build(TestComplexCreateVariance.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateVariantCreateAllParameters() throws Exception {
        TestVariantCreate variant = TestVariantCreate.create("variant-create", 100L, "varied");
        TestComplexCreateVariance expected = TestComplexCreateVariance.create("create-variant", variant);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreateVariance.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.varianceName);
        utilsBean.setProperty(builderDynaBean, "[1].class", variant.getClass().getName());
        utilsBean.setProperty(builderDynaBean, "[1].[0]", variant.variantCreateName);
        utilsBean.setProperty(builderDynaBean, "[1].[1]", variant.longClass);
        utilsBean.setProperty(builderDynaBean, "[1].[2]", variant.varyString);

        TestComplexCreateVariance actual = builderDynaBean.build(TestComplexCreateVariance.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateVariantBuilderAllParametersPrefixWithJoiner() throws Exception {
        TestVariantBuilder variant = TestVariantBuilder.builder().variantBuilderName("variant-build").intClass(20)
                .testEnum(TestEnum.Blue).build();
        TestComplexCreateVariance expected = TestComplexCreateVariance.create("builder-variant-prefix", variant);

        String prefix = variant.getClass().getEnclosingClass().getName() + "$";
        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreateVariance.class, convertUtilsBean,
                prefix);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.varianceName);
        utilsBean.setProperty(builderDynaBean, "[1].class", variant.getClass().getSimpleName());
        utilsBean.setProperty(builderDynaBean, "[1].variantBuilderName", variant.variantBuilderName);
        utilsBean.setProperty(builderDynaBean, "[1].intClass", variant.intClass);
        utilsBean.setProperty(builderDynaBean, "[1].testEnum", variant.testEnum);

        TestComplexCreateVariance actual = builderDynaBean.build(TestComplexCreateVariance.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateVariantBuilderAllParametersPrefixWithOutJoiner() throws Exception {
        TestVariantBuilder variant = TestVariantBuilder.builder().variantBuilderName("variant-build").intClass(20)
                .testEnum(TestEnum.Blue).build();
        TestComplexCreateVariance expected = TestComplexCreateVariance.create("builder-variant-prefix", variant);

        String prefix = variant.getClass().getEnclosingClass().getName();
        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreateVariance.class, convertUtilsBean,
                prefix);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.varianceName);
        utilsBean.setProperty(builderDynaBean, "[1].class", variant.getClass().getSimpleName());
        utilsBean.setProperty(builderDynaBean, "[1].variantBuilderName", variant.variantBuilderName);
        utilsBean.setProperty(builderDynaBean, "[1].intClass", variant.intClass);
        utilsBean.setProperty(builderDynaBean, "[1].testEnum", variant.testEnum);

        TestComplexCreateVariance actual = builderDynaBean.build(TestComplexCreateVariance.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexCreateVariantInvalidVariantClass() throws Exception {
        String invalidClass = "invalid-class";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("Unable to load class"));
        thrown.expectMessage(containsString(invalidClass));
        thrown.expectMessage(containsString("Attempted"));

        TestComplexCreateVariance expected = TestComplexCreateVariance.create("builder-variant-prefix", null);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreateVariance.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.varianceName);
        utilsBean.setProperty(builderDynaBean, "[1].class", invalidClass);
    }

    @Test
    public void testComplexCreateVariantBadLoadClass() throws Exception {
        thrown.expect(ExceptionInInitializerError.class);
        thrown.expectCause(instanceOf(BadClassException.class));
        TestComplexCreateVariance expected = TestComplexCreateVariance.create("builder-variant-prefix", null);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestComplexCreateVariance.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "[0]", expected.varianceName);
        utilsBean.setProperty(builderDynaBean, "[1].class", getClass().getName() + "$BadClass");
    }

    @Test
    public void testComplexRootAllParameters() throws Exception {
        TestSimpleBuilder simpleBuilder = TestSimpleBuilder.builder().stringL1("simple-l1").longVal(20L).build();
        TestRootClass expected = TestRootClass.builder().intVal(10).stringVal("root").testEnum(TestEnum.Red)
                .testComplexCreate(TestComplexCreate.create("real",
                        TestSimpleBuilder.builder().stringL1("complex-l1").longVal(10L).build()))
                .testSimpleBuilder(simpleBuilder).testSimpleCreate(TestSimpleCreate.create("first", "last")).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestRootClass.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "intVal", expected.intVal);
        utilsBean.setProperty(builderDynaBean, "stringVal", expected.stringVal);
        utilsBean.setProperty(builderDynaBean, "testEnum", expected.testEnum);
        utilsBean.setProperty(builderDynaBean, "testComplexCreate.[0]", expected.testComplexCreate.realName);
        utilsBean.setProperty(builderDynaBean, "testComplexCreate.[1].stringL1",
                expected.testComplexCreate.test1.stringL1);
        utilsBean.setProperty(builderDynaBean, "testComplexCreate.[1].longVal",
                expected.testComplexCreate.test1.longVal);
        utilsBean.setProperty(builderDynaBean, "testSimpleBuilder.class", TestSimpleBuilder.class.getName());
        utilsBean.setProperty(builderDynaBean, "testSimpleBuilder.stringL1", simpleBuilder.stringL1);
        utilsBean.setProperty(builderDynaBean, "testSimpleBuilder.longVal", simpleBuilder.longVal);
        utilsBean.setProperty(builderDynaBean, "testSimpleCreate.[0]", expected.testSimpleCreate.firstName);
        utilsBean.setProperty(builderDynaBean, "testSimpleCreate.[1]", expected.testSimpleCreate.lastName);

        TestRootClass actual = builderDynaBean.build(TestRootClass.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexRootNoParameters() throws Exception {
        TestRootClass expected = TestRootClass.builder().build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestRootClass.class, convertUtilsBean);

        TestRootClass actual = builderDynaBean.build(TestRootClass.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexRootTopLevelOnly() throws Exception {
        TestRootClass expected = TestRootClass.builder().intVal(10).stringVal("root").testEnum(TestEnum.Red).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestRootClass.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "intVal", expected.intVal);
        utilsBean.setProperty(builderDynaBean, "stringVal", expected.stringVal);
        utilsBean.setProperty(builderDynaBean, "testEnum", expected.testEnum);

        TestRootClass actual = builderDynaBean.build(TestRootClass.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testSupplierNotUsed() throws Exception {
        TestVariantBuilder variant = TestVariantBuilder.builder().testEnum(TestEnum.Green).intClass(10)
                .variantBuilderName("variant-supplier").build();
        TestSupplierClass expected = TestSupplierClass.builder().variantClass(variant).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSupplierClass.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "variantClass.class", variant.getClass().getName());
        utilsBean.setProperty(builderDynaBean, "variantClass.testEnum", variant.testEnum);
        utilsBean.setProperty(builderDynaBean, "variantClass.intClass", variant.intClass);
        utilsBean.setProperty(builderDynaBean, "variantClass.variantBuilderName", variant.variantBuilderName);

        TestSupplierClass actual = builderDynaBean.build(TestSupplierClass.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testConsumerMethodsNotExposed() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("Unknown property: mutator"));

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSupplierClass.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "mutator", "test-value");
    }

    @Test
    public void testAttemptToBuildForWrongClass() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("cannot be assigned to"));
        thrown.expectMessage(containsString(TestVariantCreate.class.getName()));
        thrown.expectMessage(containsString(TestVariantBuilder.class.getName()));

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestVariantBuilder.class, convertUtilsBean);
        builderDynaBean.build(TestVariantCreate.class);
    }

    @Test
    public void testVariantBuildsToSuperType() throws Exception {
        TestVariantBuilder expected = TestVariantBuilder.builder().intClass(10).testEnum(TestEnum.Green)
                .variantBuilderName("variant-super").build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestInterface.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "class", expected.getClass().getName());
        utilsBean.setProperty(builderDynaBean, "intClass", expected.intClass);
        utilsBean.setProperty(builderDynaBean, "testEnum", expected.testEnum);
        utilsBean.setProperty(builderDynaBean, "variantBuilderName", expected.variantBuilderName);

        TestInterface actual = builderDynaBean.build(TestInterface.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testEmptyPropertyHandler() throws Exception {
        String emptyPropertyValue = "test-property";
        TestVariantCreate expected = TestVariantCreate.create(emptyPropertyValue, (long) emptyPropertyValue.length(),
                emptyPropertyValue + "-vary");
        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestInterface.class, convertUtilsBean,
                s -> TestVariantCreate.create(s, (long) s.length(), s + "-vary"));
        utilsBean.setProperty(builderDynaBean, "", emptyPropertyValue);

        TestInterface actual = builderDynaBean.build(TestInterface.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testEmptyPropertyHandlerThrowsAfterUse() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(containsString("When a property handler is resolved further properties may not be set."));

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestInterface.class, convertUtilsBean,
                s -> TestVariantCreate.create("test", 10, "test"));
        utilsBean.setProperty(builderDynaBean, "", "test");
        utilsBean.setProperty(builderDynaBean, "[0]", "test");
    }

    @Test
    public void testEmptyPropertyReturnsInvalidObject() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString(TestEnum.class.getName()));
        thrown.expectMessage(containsString(TestInterface.class.getName()));
        thrown.expectMessage(containsString("cannot be assigned to"));

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestInterface.class, convertUtilsBean,
                s -> TestEnum.Green);

        utilsBean.setProperty(builderDynaBean, "", "test");

        builderDynaBean.build(TestInterface.class);
    }

    @Test
    public void testSimpleArrayValues() throws Exception {
        SimpleArrayClassVariant expected = SimpleArrayClassVariant.builder().ints(new Integer[] { 1, 2, 3 })
                .variantName("simple-array").longs(new Long[] { 1L, 2L, 3L }).strings(new String[] { "a", "b", "c" })
                .build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(SimpleArrayClassVariant.class, convertUtilsBean);
        utilsBean.setProperty(builderDynaBean, "variantName", expected.variantName);
        for (int i = 0; i < expected.strings.length; ++i) {
            utilsBean.setProperty(builderDynaBean, "strings[" + i + "]", expected.strings[i]);
        }

        for (int i = 0; i < expected.ints.length; ++i) {
            utilsBean.setProperty(builderDynaBean, "ints[" + i + "]", expected.ints[i]);
        }

        for (int i = 0; i < expected.longs.length; ++i) {
            utilsBean.setProperty(builderDynaBean, "longs[" + i + "]", expected.longs[i]);
        }

        SimpleArrayClassVariant actual = builderDynaBean.build(SimpleArrayClassVariant.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexArrayValuesBuilder() throws Exception {
        TestVariantBuilder variant1 = TestVariantBuilder.builder().variantBuilderName("variant-1")
                .testEnum(TestEnum.Green).intClass(10).build();
        TestVariantBuilder variant2 = TestVariantBuilder.builder().variantBuilderName("variant-2")
                .testEnum(TestEnum.Blue).intClass(20).build();
        ComplexArrayClassVariant expected = ComplexArrayClassVariant.builder().variantName("complex-test")
                .tests(new TestInterface[] { variant1, variant2 }).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(ComplexArrayClassVariant.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "variantName", expected.variantName);
        utilsBean.setProperty(builderDynaBean, "tests[0].class", TestVariantBuilder.class.getName());
        utilsBean.setProperty(builderDynaBean, "tests[0].variantBuilderName", variant1.variantBuilderName);
        utilsBean.setProperty(builderDynaBean, "tests[0].intClass", variant1.intClass);
        utilsBean.setProperty(builderDynaBean, "tests[0].testEnum", variant1.testEnum);

        utilsBean.setProperty(builderDynaBean, "tests[1].class", TestVariantBuilder.class.getName());
        utilsBean.setProperty(builderDynaBean, "tests[1].variantBuilderName", variant2.variantBuilderName);
        utilsBean.setProperty(builderDynaBean, "tests[1].intClass", variant2.intClass);
        utilsBean.setProperty(builderDynaBean, "tests[1].testEnum", variant2.testEnum);

        ComplexArrayClassVariant actual = builderDynaBean.build(ComplexArrayClassVariant.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testComplexArrayValuesCreate() throws Exception {
        TestVariantCreate variant1 = TestVariantCreate.create("variant-1", 10L, "vary-1");
        TestVariantCreate variant2 = TestVariantCreate.create("variant-2", 20L, "vary-2");

        ComplexArrayClassVariant expected = ComplexArrayClassVariant.builder().variantName("create-test")
                .tests(new TestInterface[] { variant1, variant2 }).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(ComplexArrayClassVariant.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "variantName", expected.variantName);
        utilsBean.setProperty(builderDynaBean, "tests[0].class", variant1.getClass().getName());
        utilsBean.setProperty(builderDynaBean, "tests[0].[0]", variant1.variantCreateName);
        utilsBean.setProperty(builderDynaBean, "tests[0].[1]", variant1.longClass);
        utilsBean.setProperty(builderDynaBean, "tests[0].[2]", variant1.varyString);

        utilsBean.setProperty(builderDynaBean, "tests[1].class", variant2.getClass().getName());
        utilsBean.setProperty(builderDynaBean, "tests[1].[0]", variant2.variantCreateName);
        utilsBean.setProperty(builderDynaBean, "tests[1].[1]", variant2.longClass);
        utilsBean.setProperty(builderDynaBean, "tests[1].[2]", variant2.varyString);

        ComplexArrayClassVariant actual = builderDynaBean.build(ComplexArrayClassVariant.class);

        assertThat(actual, equalTo(expected));

    }

    @Test
    public void testComplexArrayValuesMixed() throws Exception {
        TestInterface[] variants = new TestInterface[10];
        for (int i = 0; i < variants.length; ++i) {
            if (i % 2 == 0) {
                variants[i] = TestVariantCreate.create("create-variant-" + i, i + 5, "vary-" + i);
            } else {
                variants[i] = TestVariantBuilder.builder().testEnum(TestEnum.values()[i % TestEnum.values().length])
                        .intClass(i).variantBuilderName("builder-variant-" + i).build();
            }
        }

        ComplexArrayClassVariant expected = ComplexArrayClassVariant.builder().variantName("large-complex")
                .tests(variants).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(ComplexArrayClassVariant.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "variantName", expected.variantName);
        for (int i = 0; i < variants.length; ++i) {
            String prefix = "tests[" + i + "].";
            TestInterface variant = variants[i];
            if (variant instanceof TestVariantCreate) {
                TestVariantCreate create = (TestVariantCreate) variant;
                utilsBean.setProperty(builderDynaBean, prefix + "class", create.getClass().getName());
                utilsBean.setProperty(builderDynaBean, prefix + "[0]", create.variantCreateName);
                utilsBean.setProperty(builderDynaBean, prefix + "[1]", create.longClass);
                utilsBean.setProperty(builderDynaBean, prefix + "[2]", create.varyString);
            } else if (variant instanceof TestVariantBuilder) {
                TestVariantBuilder builder = (TestVariantBuilder) variant;
                utilsBean.setProperty(builderDynaBean, prefix + "class", builder.getClass().getName());
                utilsBean.setProperty(builderDynaBean, prefix + "variantBuilderName", builder.variantBuilderName);
                utilsBean.setProperty(builderDynaBean, prefix + "intClass", builder.intClass);
                utilsBean.setProperty(builderDynaBean, prefix + "testEnum", builder.testEnum);
            } else {
                fail("Unknown variant " + variants[i].getClass().getName());
            }
        }

        ComplexArrayClassVariant actual = builderDynaBean.build(ComplexArrayClassVariant.class);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testInvalidBuilderCreateClassBuild() throws Exception {
        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestInterface.class, convertUtilsBean);

        TestInterface actual = builderDynaBean.build(TestInterface.class);

        assertThat(actual, nullValue());
    }

    @Test
    public void testInvalidBuilderCreateClassSetProperty() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(containsString("Unable to to introspect or handle"));
        thrown.expectMessage(containsString(TestInterface.class.getName()));
        thrown.expectMessage(containsString("as it doesn't have a builder or create method"));

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestInterface.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "testProperty", "test");
    }

    @Test
    public void testSetMapAccessThrowsException() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage(BuilderDynaBean.NO_MAP_ACCESS_SUPPORT);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "stringL1(value)", "test");
    }

    @Test
    public void testGetMapAccessThrowsException() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage(BuilderDynaBean.NO_MAP_ACCESS_SUPPORT);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);
        //
        // We directly access the get method as there is no way to trigger utilsBean to access it
        //
        builderDynaBean.get("stringL1", "value");
    }

    @Test
    public void testRemoveThrowsException() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage(BuilderDynaBean.NO_MAP_ACCESS_SUPPORT);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);
        //
        // We directly access the remove method as there is no way to trigger utilsBean to access it
        //
        builderDynaBean.remove("stringL1", "value");
    }

    @Test
    public void testContainsThrowsException() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage(BuilderDynaBean.NO_MAP_ACCESS_SUPPORT);

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);
        //
        // We directly access the remove method as there is no way to trigger utilsBean to access it
        //
        builderDynaBean.contains("stringL1", "value");
    }

    @Test
    public void testAdditionalMutators() throws Exception {
        TestSimpleBuilder expected = TestSimpleBuilder.builder().stringL1("test").longVal(10L).build();

        BuilderDynaBean builderDynaBean = new BuilderDynaBean(TestSimpleBuilder.class, convertUtilsBean);

        utilsBean.setProperty(builderDynaBean, "stringL1", expected.stringL1);

        TestSimpleBuilder actual = builderDynaBean.build(TestSimpleBuilder.class,
                b -> ((TestSimpleBuilder.TestSimpleBuilderBuilder) b).longVal(expected.longVal));

        assertThat(actual, equalTo(expected));
    }

    public enum TestEnum {
        Red, Green, Blue
    }

    public interface TestInterface {

    }

    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class TestSimpleCreate {
        private final String firstName;
        private final String lastName;

        public static TestSimpleCreate create(String firstName, String lastName) {
            return new TestSimpleCreate(firstName, lastName);
        }
    }

    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class TestComplexCreate {
        private final String realName;
        private final TestSimpleBuilder test1;

        public static TestComplexCreate create(String realName, TestSimpleBuilder test1) {
            return new TestComplexCreate(realName, test1);
        }
    }

    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class TestComplexCreateVariance {
        private final String varianceName;
        private final TestInterface variant;

        public static TestComplexCreateVariance create(String varianceName, TestInterface variant) {
            return new TestComplexCreateVariance(varianceName, variant);
        }
    }

    @Builder
    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    public static class TestSimpleBuilder implements TestInterface {
        private String stringL1;
        private long longVal;
    }

    @Builder
    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    public static class TestVariantBuilder implements TestInterface {
        private String variantBuilderName;
        private TestEnum testEnum;
        private Integer intClass;
    }

    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class TestVariantCreate implements TestInterface {
        private final String variantCreateName;
        private final long longClass;
        private final String varyString;

        public static TestVariantCreate create(String variantCreateName, long longClass, String varyString) {
            return new TestVariantCreate(variantCreateName, longClass, varyString);
        }
    }

    @Builder
    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    public static class TestRootClass {
        private String stringVal;
        private int intVal;
        private TestEnum testEnum;
        TestSimpleCreate testSimpleCreate;
        TestComplexCreate testComplexCreate;
        TestSimpleBuilder testSimpleBuilder;
    }

    @ToString
    @EqualsAndHashCode
    public static class TestSupplierClass {
        private TestInterface variantClass;

        public static TestSupplierClassBuilder builder() {
            return new TestSupplierClassBuilder();
        }
    }

    @Builder
    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    public static class SimpleArrayClassVariant implements TestInterface {
        private String variantName;
        private String[] strings;
        private Integer[] ints;
        private Long[] longs;
    }

    @Builder
    @Accessors(fluent = true)
    @ToString
    @EqualsAndHashCode
    public static class ComplexArrayClassVariant implements TestInterface {
        private String variantName;
        private TestInterface[] tests;
    }

    public static class TestSupplierClassBuilder {
        private TestSupplierClass testSupplierClass = new TestSupplierClass();

        public TestSupplierClassBuilder variantClass(TestInterface testInterface) {
            testSupplierClass.variantClass = testInterface;
            return this;
        }

        public TestSupplierClassBuilder variantClass(Supplier<TestInterface> supplier) {
            throw new IllegalStateException("Supplier method should not be used.");
        }

        public TestSupplierClassBuilder mutator(Consumer<TestSupplierClassBuilder> consumer) {
            consumer.accept(this);
            return this;
        }

        public TestSupplierClass build() {
            return testSupplierClass;
        }
    }

    public static class BadClassException extends RuntimeException {
        public BadClassException(String message) {
            super(message);
        }
    }

    public static class BadClass {
        static {
            if (BuilderDynaBeanTest.isBad) {
                throw new BadClassException("This is a bad class");
            }
        }

        public String name = "default";

    }

}