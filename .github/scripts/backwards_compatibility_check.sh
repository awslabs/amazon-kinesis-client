#!/bin/bash

TRUE=1
FALSE=0

REMOVED_METHODS_FLAG=$FALSE

KCL_MAVEN_DIR=~/.m2/repository/software/amazon/kinesis/amazon-kinesis-client

#  clear the Maven directory so that the latest release will be the only version in the Maven directory after running mvn dependency:get
rm -rf $KCL_MAVEN_DIR
mvn -B dependency:get -Dartifact=software.amazon.kinesis:amazon-kinesis-client:LATEST
LATEST_VERSION=$(ls $KCL_MAVEN_DIR | grep '[0-9].[0-9].[0-9]')
LATEST_JAR=$KCL_MAVEN_DIR/$LATEST_VERSION/amazon-kinesis-client-$LATEST_VERSION.jar

# get the current jar (i.e. the jar that is pending review)
mvn -B install -DskipTests
CURRENT_VERSION=$(mvn -q  -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
CURRENT_JAR=$KCL_MAVEN_DIR/$CURRENT_VERSION/amazon-kinesis-client-$CURRENT_VERSION.jar

echo "Checking if methods in KCL version $LATEST_VERSION (latest release) were removed in version $CURRENT_VERSION (current)"

LATEST_CLASSES=$(jar tf $LATEST_JAR | grep .class | tr / . |  sed 's/\.class$//' | sort)
for CLASS in $LATEST_CLASSES
do
  # Skip classes which are not public. Only public class should be checked since other classes will not break backwards compatibility.
  CLASS_DEFINITION=$(javap -classpath $LATEST_JAR $CLASS | head -2 | tail -1)

  # Skip classes with the KinesisClientInternalApi annotation. These classes are subject to breaking backwards compatibility.
  INTERNAL_API_RESULT=$(javap -v -classpath $LATEST_JAR $CLASS | grep KinesisClientInternalApi)

  if [[ $CLASS_DEFINITION != *"public"* || $INTERNAL_API_RESULT != "" ]]
  then
    continue
  fi

  # check if any methods were removed
  LATEST_METHODS=$(javap -classpath $LATEST_JAR $CLASS)
  CURRENT_METHODS=$(javap -classpath $CURRENT_JAR $CLASS)
  REMOVED_METHODS=$(diff <(echo "$LATEST_METHODS") <(echo "$CURRENT_METHODS") | grep '^<')
  if [[ $REMOVED_METHODS != "" ]]
  then
    REMOVED_METHODS_FLAG=$TRUE
    echo "Methods removed in class ${CLASS##*.}"
    echo "$REMOVED_METHODS"
  fi
done

if [[ $REMOVED_METHODS_FLAG == $TRUE ]]
then
  echo "KCL version $CURRENT_VERSION is not backwards compatible with version $LATEST_VERSION. See output above for removed packages/methods."
  exit 1
fi