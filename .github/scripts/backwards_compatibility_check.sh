#!/bin/bash

TRUE=1
FALSE=0
KCL_MAVEN_DIR=~/.m2/repository/software/amazon/kinesis/amazon-kinesis-client

REMOVED_METHODS_FLAG=$FALSE
LATEST_VERSION=""
LATEST_JAR=""
CURRENT_VERSION=""
CURRENT_JAR=""

# Get the JAR from the latest version release on Maven.
get_latest_jar() {
  #  clear the directory so that the latest release will be the only version in the Maven directory after running mvn dependency:get
  rm -rf "$KCL_MAVEN_DIR"
  mvn -B dependency:get -Dartifact=software.amazon.kinesis:amazon-kinesis-client:LATEST
  LATEST_VERSION=$(ls "$KCL_MAVEN_DIR" | grep -E '[0-9]+.[0-9]+.[0-9]+')
  LATEST_JAR=$KCL_MAVEN_DIR/$LATEST_VERSION/amazon-kinesis-client-$LATEST_VERSION.jar
}

# Get the JAR with the changes that need to be verified.
get_current_jar() {
  mvn -B install -Dmaven.test.skip=true
  CURRENT_VERSION=$(mvn -q  -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
  CURRENT_JAR=$KCL_MAVEN_DIR/$CURRENT_VERSION/amazon-kinesis-client-$CURRENT_VERSION.jar
}

is_new_minor_release() {
  local latest_minor_version=$(echo "$LATEST_VERSION" | cut -d . -f 2)
  local current_minor_version=$(echo "$CURRENT_VERSION" | cut -d . -f 2)
  [[ "$latest_minor_version" != "$current_minor_version" ]]
  return $?
}

# Skip classes with the KinesisClientInternalApi annotation. These classes are subject to breaking backwards compatibility.
is_kinesis_client_internal_api() {
  local current_class="$1"
  local grep_internal_api_result=$(javap -v -classpath "$LATEST_JAR" "$current_class" | grep KinesisClientInternalApi)
  [[ "$grep_internal_api_result" != "" ]]
  return $?
}

# Skip classes which are not public (e.g. package level). These classes will not break backwards compatibility.
is_non_public_class() {
  local current_class="$1"
  local class_definition=$(javap -classpath "$LATEST_JAR" "$current_class" | head -2 | tail -1)
  [[ "$class_definition" != *"public"* ]]
  return $?
}

# Ignore methods that change from abstract to non-abstract (and vice versa) if the class is an interface.
ignore_abstract_changes_in_interfaces() {
  local current_class="$1"
  local class_definition=$(javap -classpath "$LATEST_JAR" "$current_class" | head -2 | tail -1)
  if [[ $class_definition == *"interface"* ]]
  then
    LATEST_METHODS=${LATEST_METHODS// abstract / }
    CURRENT_METHODS=${CURRENT_METHODS// abstract / }
  fi
}

# Checks if there are any methods in the latest version that were removed in the current version.
find_removed_methods() {
  echo "Checking if methods in current version (v$CURRENT_VERSION) were removed from latest version (v$LATEST_VERSION)"
  if is_new_minor_release
  then
    echo "New minor release is being performed. Ignoring changes in classes marked with @KinesisClientInternalApi annotation."
  fi
  local latest_classes=$(
    jar tf $LATEST_JAR |
    grep .class |
    tr / . |
    sed 's/\.class$//' |
    # skip generated proto classes since these have a lot of inherited methods
    # that are not outputted by javap. besides, generated java code is not a
    # good indicator of proto compatibility- it will not capture reserved
    # tags or deprecated fields.
    grep -v 'software\.amazon\.kinesis\.retrieval\.kpl\.Messages')
  for class in $latest_classes
  do
    if (is_kinesis_client_internal_api "$class" && is_new_minor_release) || is_non_public_class "$class"
    then
      continue
    fi

    LATEST_METHODS=$(javap -classpath "$LATEST_JAR" "$class")
    CURRENT_METHODS=$(javap -classpath "$CURRENT_JAR" "$class")

    ignore_abstract_changes_in_interfaces "$class"

    local removed_methods=$(diff <(echo "$LATEST_METHODS") <(echo "$CURRENT_METHODS") | grep '^<')

    # ignore synthetic access methods - these are not available to users and will not break backwards compatibility
    removed_methods=$(echo "$removed_methods" | grep -v "access\$[0-9]\+")

    if [[ "$removed_methods" != "" ]]
    then
      REMOVED_METHODS_FLAG=$TRUE
      if is_kinesis_client_internal_api "$class"
      then
        echo "Found removed methods in class with @KinesisClientInternalApi annotation. To resolve these issues, upgrade the current minor version or address these changes."
      fi
      echo "$class does not have method(s):"
      echo "$removed_methods"
    fi
  done
}

get_backwards_compatible_result() {
  if [[ $REMOVED_METHODS_FLAG == $TRUE ]]
  then
    echo "Current KCL version $CURRENT_VERSION is not backwards compatible with version $LATEST_VERSION. See output above for removed packages/methods."
    exit 1
  else
    echo "Current KCL version $CURRENT_VERSION is backwards compatible with version $LATEST_VERSION."
    exit 0
  fi
}

main() {
  get_latest_jar
  get_current_jar
  find_removed_methods
  get_backwards_compatible_result
}

main
