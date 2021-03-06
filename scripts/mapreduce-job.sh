#!/bin/bash
#
# MapReduce 실습용 소스

set -e -o pipefail

##################################
#
# Global setting & arguments
#
##################################

source /home/student/projects/ygbae/course-hadoop/scripts/common.in


#######################################
# Word count 작업을 실행한다.
# -D mapreduce.job.reduces=1
#######################################
word_count()
{
	local input=$1
	local output=$2
	local num_reducers=2

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.wordcount.WordCountJob \
		-D mapreduce.job.reduces=${num_reducers} \
		-D mapred.reduce.slowstart.completed.maps=0.9 \
		--input "${input}" \
		--output "${output}";
}

#######################################
# Word count 작업을 Spark로 실행한다.
#######################################
word_count_by_spark()
{
	local input=$1
	local output=$2

	$HADOOP fs -rmr ${output} >& /dev/null || true

	spark-submit \
	--class com.nextmining.course.hadoop.wordcount.WordCountSparkJob \
	--num-executors 3 \
	--master yarn-client \
	${JOB_JAR} \
	--input ${input} \
	--output ${output}
}

#######################################
# NCDC데이터에서 연도별 기온 데이터를 기온기준으로 내림차순으로 전체 정렬을 한다.
#
#######################################
ncdc_partial_sort()
{
	local input=$1
	local output=$2
	local num_reducers=3

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcPartialSortJob \
	    -D mapreduce.job.reduces=${num_reducers} \
		--input "${input}" \
		--output "${output}";
}

ncdc_total_sort()
{
	local input=$1
	local output=$2
	local num_reducers=3

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcTotalSortJob \
	    -D mapreduce.job.reduces=${num_reducers} \
		--input "${input}" \
		--output "${output}";
}

ncdc_secondary_sort()
{
	local input=$1
	local output=$2
	local num_reducers=3

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcSecondarySortJob \
	    -D mapreduce.job.reduces=${num_reducers} \
		--input "${input}" \
		--output "${output}";
}

#######################################
# NCDC데이터에서 기상청 이름을 조인한다.
#
#######################################
ncdc_join()
{
	local inputNcdc=$1
	local inputStation=$2
	local output=$3
	local num_reducers=3

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcJoinJob \
	    -D mapreduce.job.reduces=${num_reducers} \
		--inputNcdc "${inputNcdc}" \
		--inputStation "${inputStation}" \
		--output "${output}";
}

#######################################
# NCDC데이터에서 분산캐시를 이용해서 기상청 이름을 입력한다.
#
#######################################
ncdc_station_name()
{
	local input=$1
	local output=$2
	local num_reducers=3

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcStationNameJob \
	    -D mapreduce.job.reduces=${num_reducers} \
	    -files "/home/student/data/input/ncdc/metadata/stations-fixed-width.txt#stations-fixed-width.txt" \
		--input "${input}" \
		--output "${output}" \
		--minTemperature "0";
}

#######################################
# NCDC데이터에서 연도별 최고기온을 분석해 보자.
#
#######################################
ncdc_max_temperature_by_year()
{
	local input=$1
	local output=$2
	local num_reducers=1

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcMaxTemperatureByYearJob \
	    -D mapreduce.job.reduces=${num_reducers} \
		--input "${input}" \
		--output "${output}";
}

#######################################
# NCDC데이터에서 연도별/기상청별 최고기온을 분석한다.
#
#######################################
ncdc_max_temperature_by_year_station()
{
	local input=$1
	local output=$2
	local num_reducers=2

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcMaxTemperatureByYearStationJob \
	    -D mapreduce.job.reduces=${num_reducers} \
	    -files "/home/student/data/input/ncdc/metadata/stations-fixed-width.txt#stations-fixed-width.txt" \
		--input "${input}" \
		--output "${output}";
}

ncdc_max_temperature_by_year_station1()
{
	local input=$1
	local output=$2
	local num_reducers=2

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.ncdc.NcdcMaxTemperatureByYearStationJob1 \
	    -D mapreduce.job.reduces=${num_reducers} \
	    -files "/home/student/data/input/ncdc/metadata/stations-fixed-width.txt#stations-fixed-width.txt" \
		--input "${input}" \
		--output "${output}";
}

#######################################
# LINE 아이폰 앱스토어 사용자리뷰 데이터를 분석한다.
#
#######################################
line_ios_review_word_count()
{
	local input=$1
	local output=$2
	local num_reducers=10

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.linereview.LineReviewWordCountByRateJob \
	    -D mapreduce.job.reduces=${num_reducers} \
	    -files "${PROJECT_HOME}/dics/stopwords_en.txt#stopwords_en.txt" \
		--input "${input}" \
		--output "${output}";
}

line_ios_review_word_count_sort()
{
	local input=$1
	local output=$2
	local num_reducers=5

	$HADOOP fs -rmr ${output} >& /dev/null || true

	$HADOOP jar $JOB_JAR com.nextmining.course.hadoop.linereview.LineReviewWordCountSortJob \
	    -D mapreduce.job.reduces=${num_reducers} \
		--input "${input}" \
		--output "${output}";
}

##################################
#
# Main
#
##################################

run_word_count() {
	word_count "/user/student/data/docs/1400-8.txt" "${MY_HDFS_HOME}/word_count"
}

run_word_count_by_spark() {
    word_count_by_spark "/user/student/data/docs/1400-8.txt" "${MY_HDFS_HOME}/word_count_by_spark1"
}
run_ncdc_partial_sort() {
    ncdc_partial_sort "/user/student/data/ncdc/all" "${MY_HDFS_HOME}/ncdc/partial_sort"
}

run_ncdc_total_sort() {
    ncdc_total_sort "/user/student/data/ncdc/all" "${MY_HDFS_HOME}/ncdc/total_sort"
}

run_ncdc_secondary_sort() {
    ncdc_secondary_sort "/user/student/data/ncdc/all" "${MY_HDFS_HOME}/ncdc/secondary_sort"
}

run_ncdc_join() {
    ncdc_join "/user/student/data/ncdc/all" "/user/student/data/ncdc/metadata/stations-fixed-width.txt" "${MY_HDFS_HOME}/ncdc/join"
}

run_ncdc_station_name() {
    ncdc_station_name "/user/student/data/ncdc/all" "${MY_HDFS_HOME}/ncdc/station_name"
}

run_ncdc_max_temperature_by_year() {
    ncdc_max_temperature_by_year "/user/student/data/ncdc/all" "${MY_HDFS_HOME}/ncdc/max_by_year"
}

run_ncdc_max_temperature_by_year_station() {
    ncdc_max_temperature_by_year_station "/user/student/data/ncdc/all" "${MY_HDFS_HOME}/ncdc/max_by_year_station"
}

run_ncdc_max_temperature_by_year_station1() {
    ncdc_max_temperature_by_year_station1 "/user/student/data/ncdc/all" "${MY_HDFS_HOME}/ncdc/max_by_year_station1"
}

run_line_ios_review_word_count() {
    line_ios_review_word_count "/user/student/data/line_ios_review/line_ios_review_us_20160101.txt" "${MY_HDFS_HOME}/line_ios_review/line_ios_review_us_word_count"
}

run_line_ios_review_word_count_sort() {
    line_ios_review_word_count_sort "${MY_HDFS_HOME}/line_ios_review/line_ios_review_us_word_count" "${MY_HDFS_HOME}/line_ios_review/line_ios_review_us_word_count_sort"
}

run_line_ios_review() {
    run_line_ios_review_word_count
    run_line_ios_review_word_count_sort
}

CMD=$1
shift
$CMD $*