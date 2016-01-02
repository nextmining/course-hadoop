#!/bin/bash
#
# This script builds a single jar file.

set -e -o pipefail

#######################################
#
# Global setting & arguments
#
#######################################

# 아래 MY_HOME를 자신의 작업 디렉토리에 맞게 변경해 주세요.
MY_HOME=/home/lineplus/ygbae

PROJECT_NAME=course-hadoop
PROJECT_HOME=${MY_HOME}/${PROJECT_NAME}
PROJECT_VERSION=1.0.0
BUILD_DIR=${PROJECT_HOME}/build
CLASS_DIR=${BUILD_DIR}/intermediate-classes
BUILD_LIBS_DIR=${BUILD_DIR}/libs

# Prints error.
err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

# Initializes.
init() {
	echo $(date +%Y%m%d%H%M%S) " > $0 : starting...."
}

# Finishes.
end() {
	echo $(date +%Y%m%d%H%M%S) " > $0 : finished"
}

# Makes a single jar file from multiple jar files.
build_single_jar() {
	echo "Build a single jar file....."

	mkdir -p ${CLASS_DIR}
	rm -rf ${CLASS_DIR}/*
	
	# extract other jars so we can bundle them together
	cd ${CLASS_DIR}; jar xf ${PROJECT_HOME}/hadoop-libs/jackson-core-2.3.0.jar; cd ${PROJECT_HOME}
	cd ${CLASS_DIR}; jar xf ${PROJECT_HOME}/hadoop-libs/jackson-annotations-2.3.0.jar; cd ${PROJECT_HOME}
	cd ${CLASS_DIR}; jar xf ${PROJECT_HOME}/hadoop-libs/jackson-databind-2.3.0.jar; cd ${PROJECT_HOME}
	cd ${CLASS_DIR}; jar xf ${PROJECT_HOME}/hadoop-libs/opennlp-maxent-3.0.2-incubating.jar; cd ${PROJECT_HOME}
	cd ${CLASS_DIR}; jar xf ${PROJECT_HOME}/hadoop-libs/opennlp-tools-1.5.2-incubating.jar; cd ${PROJECT_HOME}
	cd ${CLASS_DIR}; jar xf ${BUILD_LIBS_DIR}/${PROJECT_NAME}-${PROJECT_VERSION}.jar; cd ${PROJECT_HOME}

	
	# bundle every thing into a single jar
	cd ${BUILD_LIBS_DIR}; jar cf ${PROJECT_NAME}-job.jar -C ${CLASS_DIR} .; cd ${PROJECT_HOME}
	
	echo "Build a single jar file: done"
}

run() {
	init

	cd ${PROJECT_HOME}
	gradle build

	build_single_jar
	
	end
}

#CMD=$1
#shift
#$CMD $*

run