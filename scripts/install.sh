#!/usr/bin/env bash
export PATH=/opt/apache-maven-3.6.3/bin:$PATH
cd /home/ec2-user/kcl-2x
mvn clean install
