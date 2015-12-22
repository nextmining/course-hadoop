#!/bin/bash
#
# MapReduce 실습용 소스

set -e -o pipefail

##################################
#
# Global setting & arguments
#
##################################

source /home/lineplus/ygbae/course-hadoop/scripts/common.in


#######################################
# Word count 작업을 실행한다.
#######################################
word_count()
{
	local input=$1
	local output=$2
	local num_reducers=1

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.wordcount.WordCountJob \
		-Dmapred.reduce.tasks=${num_reducers} \
		-Dmapred.reduce.slowstart.completed.maps=0.9 \
		--input "${input}" \
		--output "${output}";
}


##################################
#
# Main
#
##################################

run_word_count() {
	word_count "/coll/twitter/kpop" "${MY_HDFS_HOME}/word_count" "3"
}

CMD=$1
shift
$CMD $*