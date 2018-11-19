package software.amazon.kinesis.leases;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.leases.DynamoUtils;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

public class DynamoUtilsTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void safeGetStringInput1NotNullOutputNull() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put("", null);
    final String key = "";

    // Act
    final String retval = DynamoUtils.safeGetString(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetStringInput1NotNullOutputNull2() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put(null, null);
    final String key = "#";

    // Act
    final String retval = DynamoUtils.safeGetString(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetStringInput1NotNullOutputNull3() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put("!-\"%", null);
    final String key = "!-\"%";

    // Act
    final String retval = DynamoUtils.safeGetString(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetStringInput1NotNullOutputNull6() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put(null, null);
    final String key = "";

    // Act
    final String retval = DynamoUtils.safeGetString(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetStringInput0NotNullOutputNull() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    final String key = "";

    // Act
    final String retval = DynamoUtils.safeGetString(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetSSInput1NotNullOutput() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put("!!!!!!!!! ", null);
    final String key = "!!!!!!!!!";

    // Act
    final List<String> retval = DynamoUtils.safeGetSS(dynamoRecord, key);

    // Assert result
    final ArrayList<String> arrayList = new ArrayList<String>();
    Assert.assertEquals(arrayList, retval);
  }

  @Test
  public void safeGetSSInput1NotNullOutput2() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put(" q c", null);
    final String key = "ca`c";

    // Act
    final List<String> retval = DynamoUtils.safeGetSS(dynamoRecord, key);

    // Assert result
    final ArrayList<String> arrayList = new ArrayList<String>();
    Assert.assertEquals(arrayList, retval);
  }

  @Test
  public void safeGetSSInput1NotNullOutput3() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put(null, null);
    final String key = "";

    // Act
    final List<String> retval = DynamoUtils.safeGetSS(dynamoRecord, key);

    // Assert result
    final ArrayList<String> arrayList = new ArrayList<String>();
    Assert.assertEquals(arrayList, retval);
  }

  @Test
  public void safeGetSSInput0NotNullOutput() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    final String key = "";

    // Act
    final List<String> retval = DynamoUtils.safeGetSS(dynamoRecord, key);

    // Assert result
    final ArrayList<String> arrayList = new ArrayList<String>();
    Assert.assertEquals(arrayList, retval);
  }

  @Test
  public void safeGetLongInput1NotNullOutputNull() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put("``", null);
    final String key = "``";

    // Act
    final Long retval = DynamoUtils.safeGetLong(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetLongInput1NotNullOutputNull2() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put(null, null);
    final String key = "kk";

    // Act
    final Long retval = DynamoUtils.safeGetLong(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetLongInput0NotNullOutputNull3() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    final String key = "????????????????";

    // Act
    final Long retval = DynamoUtils.safeGetLong(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }

  @Test
  public void safeGetLongInput0NotNullOutputNull() {

    // Arrange
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    final String key = "?";

    // Act
    final Long retval = DynamoUtils.safeGetLong(dynamoRecord, key);

    // Assert result
    Assert.assertNull(retval);
  }
}
