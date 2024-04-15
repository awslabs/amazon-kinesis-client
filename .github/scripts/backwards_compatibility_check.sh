#!/bin/bash

TRUE=1
FALSE=0

REMOVED_PACKAGES_FLAG=$FALSE
REMOVED_METHODS_FLAG=$FALSE

KCL_MAVEN_DIR=~/.m2/repository/software/amazon/kinesis/amazon-kinesis-client

#  clear the Maven directory so that the latest release will be the only version in the Maven directory after running mvn dependency:get
rm -rf $KCL_MAVEN_DIR
mvn dependency:get -Dartifact=software.amazon.kinesis:amazon-kinesis-client:LATEST
LATEST_VERSION=$(ls $KCL_MAVEN_DIR | grep '[0-9].[0-9].[0-9]')
LATEST_JAR=$KCL_MAVEN_DIR/$LATEST_VERSION/amazon-kinesis-client-$LATEST_VERSION.jar

# get the current jar (i.e. the jar that is pending review)
mvn -B install -DskipTests
CURRENT_VERSION=$(mvn -q  -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
CURRENT_JAR=$KCL_MAVEN_DIR/$CURRENT_VERSION/amazon-kinesis-client-$CURRENT_VERSION.jar

echo "Comparing KCL versions $LATEST_VERSION (latest release) to $CURRENT_VERSION (current)."

# check if any packages were removed
echo "Checking if packages in version $LATEST_VERSION were removed in $CURRENT_VERSION"
LATEST_PACKAGES=$(jar tf $LATEST_JAR | grep .class | tr / . |  sed 's/\.class$//' | sort)
CURRENT_PACKAGES=$(jar tf $CURRENT_JAR | grep .class | tr / . |  sed 's/\.class$//' | sort)
diff <(echo "$LATEST_PACKAGES") <(echo "$CURRENT_PACKAGES") | grep '^<' && REMOVED_PACKAGES_FLAG=$TRUE || echo "No packages removed in version $CURRENT_VERSION."

# check if any methods within the packages were removed
echo "Checking if methods in $LATEST_VERSION were removed in $CURRENT_VERSION"
for package in $LATEST_PACKAGES
do
  # Skip classes which are not public. Only public class should be checked since other classes will not break backwards compatibility.
  CLASS_DEFINITION=$(javap -classpath $LATEST_JAR $package | head -2 | tail -1)

  # Skip classes with the KinesisClientInternalApi annotation. These classes are subject to breaking backwards compatibility.
  INTERNAL_API_RESULT=$(javap -v -classpath $LATEST_JAR $package | grep KinesisClientInternalApi)

  if [[ $CLASS_DEFINITION != *"public"* || $INTERNAL_API_RESULT != "" ]]
  then
    continue
  fi

  # check if any methods were removed
  LATEST_METHODS=$(javap -classpath $LATEST_JAR $package)
  CURRENT_METHODS=$(javap -classpath $CURRENT_JAR $package)
  diff <(echo "$LATEST_METHODS") <(echo "$CURRENT_METHODS") | grep '^<' && REMOVED_METHODS_FLAG=$TRUE || :
done

if [[ $REMOVED_METHODS_FLAG == $FALSE ]]
then
  echo "No methods removed in version $CURRENT_VERSION."
fi

if [[ $REMOVED_PACKAGES_FLAG == $TRUE || $REMOVED_METHODS_FLAG == $TRUE ]]
then
  echo "KCL version $CURRENT_VERSION is not backwards compatible with version $LATEST_VERSION. See output above for removed packages/methods."
  exit 1
fi