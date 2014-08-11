#!/bin/bash

export HADOOP_HOME="/home/goiri/hadoop-1.1.2"
export HADOOP_WIKIPEDIA="/home/goiri/hadoop-1.1.2-src/build/contrib/wikipedia/hadoop-wikipedia-1.1.3.jar    org.apache.hadoop.mapreduce.wikipedia.WikiPopularity"
export HADOOP_APACHE="/home/goiri/hadoop-1.1.2-src/build/contrib/wikipedia/hadoop-wikipedia-1.1.3.jar       org.apache.hadoop.mapreduce.apache.ApacheLogAnalysis"
export HADOOP_WIKIPAGERANK="/home/goiri/hadoop-1.1.2-src/build/contrib/wikipedia/hadoop-wikipedia-1.1.3.jar org.apache.hadoop.mapreduce.wikipedia.WikiPageRank"
export HADOOP_WIKILENGTHS="/home/goiri/hadoop-1.1.2-src/build/contrib/wikipedia/hadoop-wikipedia-1.1.3.jar  org.apache.hadoop.mapreduce.wikipedia.WikiLengths"

export INPUT_APACHE_LOG="/user/goiri/apachelog/"
export INPUT_WIKIPEDIA_LOG="/user/goiri/wikipedialog/"
export INPUT_WIKIPEDIA="/user/goiri/wikipedia-dump.xml.bz2"


function start_energy_monitors {
	# Kill all the previous monitors
	for PID in `ps ax | grep ssh | grep sar | awk '{print $1}'`; do kill -9 $PID; done 2> /dev/null
	# Start CPU monitors
	for SLAVE in `cat $HADOOP_HOME/conf/slaves`; do	ssh $SLAVE sar -u 1 > /tmp/cpu-$SLAVE & done 2> /dev/null
}

function stop_energy_monitors {
	# Kill masters
	for PID in `ps ax | grep ssh | grep sar | awk '{print $1}'`; do kill -9 $PID 2> /dev/null; done
	# Kill slaves
	for SLAVE in `cat $HADOOP_HOME/conf/slaves`; do ssh $SLAVE killall -9 sar; done 2> /dev/null
}

function collect_energy_monitors {
	OUTPUTPATH=$1
	if [ ! -d $OUTPUTPATH ]; then
		mkdir $OUTPUTPATH
	fi
	# Save the output
	for SLAVE in `cat $HADOOP_HOME/conf/slaves`; do
		cat /tmp/cpu-$SLAVE | awk '$3 == "all" {print $1" "$2" "100.0-$9}' > $OUTPUTPATH/cpu-$SLAVE
	done
	python getenergy.py $OUTPUTPATH
}

# Experiment parameters
RUNS=20 # To capture 95% confidence
DROPS="100 75 50 25"
SAMPLINGS="1 2 5 10 100 1000 10000" # 100000
OUTPUT_FILE="results.data"

# Header
echo "# App Run Sampling Dropping Time Energy" >> $OUTPUT_FILE

for RUN in `seq 1 $RUNS`; do
	# Apache logs
	if false; then
		for TASK in `echo dateweek size browser hack host totalsize page pagesize`; do
			# Precise
			start_energy_monitors
			TIME0=`date +%s`
			# Run
			$HADOOP_HOME/bin/hadoop jar $HADOOP_APACHE -input $INPUT_APACHE_LOG -output /user/goiri/apache-$TASK-precise-$RUN -r 10 -t $TASK -p
			# Account time
			TIME1=`date +%s`
			stop_energy_monitors
			# Calculate data
			let TIME=$TIME1-$TIME0
			ENERGY=`collect_energy_monitors /tmp/collectenergy`
			# Output
			echo "apache-$TASK $RUN precise 100 $TIME $ENERGY" >> $OUTPUT_FILE
			sleep 1
			
			# Approximations
			for SAMPLING in $SAMPLINGS; do #  100000
				for DROP in $DROPS; do
					start_energy_monitors
					TIME0=`date +%s`
					# Run
					$HADOOP_HOME/bin/hadoop jar $HADOOP_APACHE -input $INPUT_APACHE_LOG -output /user/goiri/apache-$TASK-$SAMPLING-$DROP-$RUN -r 10 -t $TASK -s $SAMPLING -n $DROP
					# Account time
					TIME1=`date +%s`
					stop_energy_monitors
					# Calculate data
					let TIME=$TIME1-$TIME0
					ENERGY=`collect_energy_monitors /tmp/collectenergy`
					# Output
					echo "This run took $TIME seconds and $ENERGY Wh"
					echo "apache-$TASK $RUN $SAMPLING $DROP $TIME $ENERGY" >> $OUTPUT_FILE
					sleep 1
				done
			done
		done
	fi

	# PageRank
	if false; then
		# Precise
		start_energy_monitors
		TIME0=`date +%s`
		# Run
		$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPAGERANK -input $INPUT_WIKIPEDIA -output /user/goiri/wiki-pagerank-precise-$RUN -r 10 -p
		# Account time
		TIME1=`date +%s`
		stop_energy_monitors
		# Calculate data
		let TIME=$TIME1-$TIME0
		ENERGY=`collect_energy_monitors /tmp/collectenergy`
		# Output
		echo "wiki-pagerank $RUN precise 100 $TIME $ENERGY" >> $OUTPUT_FILE
		
		# Approximations
		for SAMPLING in $SAMPLINGS; do
			for DROP in $DROPS; do
				start_energy_monitors
				TIME0=`date +%s`
				# Run
				$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPAGERANK -input $INPUT_WIKIPEDIA -output /user/goiri/wiki-pagerank-$SAMPLING-$DROP-$RUN -r 10 -s $SAMPLING -n $DROP
				# Account time
				TIME1=`date +%s`
				stop_energy_monitors
				# Calculate data
				let TIME=$TIME1-$TIME0
				ENERGY=`collect_energy_monitors /tmp/collectenergy`
				# Output
				echo "This run took $TIME seconds and $ENERGY Wh"
				echo "wiki-pagerank $RUN $SAMPLING $DROP $TIME $ENERGY" >> $OUTPUT_FILE
			done
		done
	fi

	# Wikipedia length
	if false; then
		# Precise
		start_energy_monitors
		TIME0=`date +%s`
		# Run
		$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKILENGTHS  -input $INPUT_WIKIPEDIA -output /user/goiri/wiki-length-precise-$RUN -r 10 -p
		# Account time
		TIME1=`date +%s`
		stop_energy_monitors
		# Calculate data
		let TIME=$TIME1-$TIME0
		ENERGY=`collect_energy_monitors /tmp/collectenergy`
		# Output
		echo "wiki-length $RUN precise 100 $TIME $ENERGY" >> $OUTPUT_FILE
		
		# Approximations
		for SAMPLING in $SAMPLINGS; do
			for DROP in $DROPS; do
				start_energy_monitors
				TIME0=`date +%s`
				# Run
				$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKILENGTHS -input $INPUT_WIKIPEDIA -output /user/goiri/wiki-length-$SAMPLING-$DROP-$RUN -r 10 -s $SAMPLING -n $DROP
				# Account time
				TIME1=`date +%s`
				stop_energy_monitors
				# Calculate data
				let TIME=$TIME1-$TIME0
				ENERGY=`collect_energy_monitors /tmp/collectenergy`
				# Output
				echo "This run took $TIME seconds and $ENERGY Wh"
				echo "wiki-length $RUN $SAMPLING $DROP $TIME $ENERGY" >> $OUTPUT_FILE
			done
		done
	fi
	
	# Wikipedia access logs
	if false; then
		#for TASK in `echo page project`; do
		for TASK in `echo page`; do
			# Precise
			start_energy_monitors
			TIME0=`date +%s`
			# Run
			$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/wiki-$TASK-precise-$RUN -r 10 -t $TASK -p
			# Account time
			TIME1=`date +%s`
			stop_energy_monitors
			# Calculate data
			let TIME=$TIME1-$TIME0
			ENERGY=`collect_energy_monitors /tmp/collectenergy`
			# Output
			echo "wiki-$TASK $RUN precise 100 $TIME $ENERGY" >> $OUTPUT_FILE
			sleep 1
			
			# Approximations
			for SAMPLING in $SAMPLINGS; do #  100000
				for DROP in $DROPS; do
					start_energy_monitors
					TIME0=`date +%s`
					# Run
					$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/wiki-$TASK-$SAMPLING-$DROP-$RUN -r 10 -t $TASK -s $SAMPLING -n $DROP
					# Account time
					TIME1=`date +%s`
					stop_energy_monitors
					# Calculate data
					let TIME=$TIME1-$TIME0
					ENERGY=`collect_energy_monitors /tmp/collectenergy`
					# Output
					echo "This run took $TIME seconds and $ENERGY Wh"
					echo "wiki-$TASK $RUN $SAMPLING $DROP $TIME $ENERGY" >> $OUTPUT_FILE
					sleep 1
				done
			done
		done
	fi

	# Wikipedia access log target error
	if true; then
		#for TASK in `echo project page`; do
		for TASK in `echo project`; do
			if false; then
				# Precise
				start_energy_monitors
				TIME0=`date +%s`
				# Run
				$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/wiki-$TASK-target-precise-$RUN -r 10 -t $TASK -p
				# Account time
				TIME1=`date +%s`
				stop_energy_monitors
				# Calculate data
				let TIME=$TIME1-$TIME0
				ENERGY=`collect_energy_monitors /tmp/collectenergy`
				# Output
				echo "wiki-$TASK-target $RUN precise $TIME $ENERGY" >> $OUTPUT_FILE
			fi
			
			# Target
			for TARGET in `echo 0.02 0.03 0.04 0.05 0.1 0.2 0.5 0.8 1.0 2.0 5.0 10.0 20.0 50.0`; do
				start_energy_monitors
				TIME0=`date +%s`
				# Run
				$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/wiki-$TASK-target-$TARGET-$RUN -r 10 -t $TASK -a -c -e $TARGET
				# Account time
				TIME1=`date +%s`
				stop_energy_monitors
				# Calculate data
				let TIME=$TIME1-$TIME0
				ENERGY=`collect_energy_monitors /tmp/collectenergy`
				# Output
				echo "This run took $TIME seconds and $ENERGY Wh"
				echo "wiki-$TASK-target $RUN $TARGET $TIME $ENERGY" >> $OUTPUT_FILE
			done
		done
	fi

	# Wikipedia access log target error after pilot
	if false; then
		for TARGET in `echo 0.2 0.5 0.8 1.0 2.0 5.0 10.0 20.0 50.0`; do
			SAMPLING=100
			if [ $TARGET == "0" ]; then
				SAMPLING=1
			elif [ $TARGET == "0.02" ]; then
				SAMPLING=1
			elif [ $TARGET == "0.03" ]; then
				SAMPLING=1
			elif [ $TARGET == "0.04" ]; then
				SAMPLING=1
			elif [ $TARGET == "0.05" ]; then
				SAMPLING=1
			elif [ $TARGET == "0.1" ]; then
				SAMPLING=16
			elif [ $TARGET == "0.2" ]; then
				SAMPLING=32
			elif [ $TARGET == "0.5" ]; then
				SAMPLING=48
			elif [ $TARGET == "0.8" ]; then
				SAMPLING=59
			elif [ $TARGET == "1.0" ]; then
				SAMPLING=72
			elif [ $TARGET == "2.0" ]; then
				SAMPLING=84
			elif [ $TARGET == "5.0" ]; then
				SAMPLING=253
			elif [ $TARGET == "10.0" ]; then
				SAMPLING=8468
			elif [ $TARGET == "20.0" ]; then
				SAMPLING=10000
			elif [ $TARGET == "50.0" ]; then
				SAMPLING=10000
			fi
			# Run the application
			for TASK in `echo page`; do
				start_energy_monitors
				TIME0=`date +%s`
				# Run
				$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/wiki-$TASK-targetpilot-$TARGET-$RUN -r 10 -t $TASK -a -c -e $TARGET -s $SAMPLING
				# Account time
				TIME1=`date +%s`
				stop_energy_monitors
				# Calculate data
				let TIME=$TIME1-$TIME0
				ENERGY=`collect_energy_monitors /tmp/collectenergy`
				# Output
				echo "This run took $TIME seconds and $ENERGY Wh"
				echo "wiki-$TASK-target $RUN $TARGET $TIME $ENERGY" >> $OUTPUT_FILE
			done
		done
	fi
done

# Pilot sample
if false; then
	for TARGET in `echo 0 0.02 0.03 0.04 0.05 0.1 0.2 0.5 0.8 1.0 2.0 5.0 10.0 20.0 50.0`; do
		if true; then
			for TASK in `echo page`; do
				start_energy_monitors
				TIME0=`date +%s`
				# Run
				$HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/wiki-$TASK-pilot-$TARGET-1 -r 10 -t $TASK -pilot -e $TARGET -s 100
				# Account time
				TIME1=`date +%s`
				stop_energy_monitors
				# Calculate data
				let TIME=$TIME1-$TIME0
				ENERGY=`collect_energy_monitors /tmp/collectenergy`
				# Output
				echo "wiki-$TASK-pilot $TARGET $TIME $ENERGY" >> $OUTPUT_FILE
			done
		fi
	done
	# Results
	# 	0	1
	# 	0.02	1
	# 	0.03	1
	# 	0.04	1
	# 	0.05	1
	# 	0.1	16
	# 	0.2	32
	# 	0.5	48
	# 	0.8	59
	# 	1.0	72
	# 	2.0	84
	# 	5.0	253
	# 	10.0	8468
	# 	20.0	10000
	# 	50.0	10000
fi



# $HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/debugwiki20 -r 10 -t project -s 80 -c -e 2

# $HADOOP_HOME/bin/hadoop jar $HADOOP_WIKIPEDIA -input $INPUT_WIKIPEDIA_LOG -output /user/goiri/wiki-page-precise -r 10 -t page -p
