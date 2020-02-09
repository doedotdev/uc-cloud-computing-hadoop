# uc-cloud-computing-hadoop
University of Cincinnati - Cloud Computing Hadoop Assignment

Python + Hadoop Map Reduce Job

On the Hadoop cluster, I have uploaded a file named “nyc-traffic.csv” at the following HDFS location:

```
/data/nyc/nyc-traffic.csv
```

This data was collected from the City of New York’s data website, and contains all reports of vehicular
incidents in New York City over a period of time. The file is roughly 175MB in size, and contains over
900,000 records.

There are a considerable number of fields, including columns with a common format that describe up to 5
vehicles that contributed to the particular incident.
Using the Hadoop streaming API (the one we demonstrated in class using the Python scripts, but you
may use any similar script that can be invoked in a similar manner using STDIN and STDOUT), build
mapper and reducer scripts that analyze the data and yield summary counts for each vehicle that
describe the total count, per vehicle type, that the vehicle type was involved in an incident. If the same
type of vehicle was involved more than once in an incident, count the vehicle twice for the purpose of the
summary statistic. Please refer to https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-VehicleCollisions/h9gi-nx95
for more information about the dataset.

## Set Up

SSH into the cluster.

```
$ ssh hornbd@hadoop-gate-0.eecs.uc.edu
```

Create the Mapper File in your user directory
```
$ nano mapper.py
```
(Don't use nano if you don't want to.)

Create the Reducer File in your user directory
```
$ nano reducer.py
```
(Don't use nano if you don't want to.)

Check your username
```
$ echo $USER
hornbd
```

## Running/ Editing the Job
I find it easiest/ best to edit the file locally then `put` it back up when ready.

Get the file:
```
$ hadoop fs -get mapper.py
$ hadoop fs -get reducer.py
```

Make your changes.
```
$ nano mapper.py
$ nano reducer.py
```

Put the file:
```
$ hdfs dfs -put -f reducer.py /user/$USER/reducer.py
$ hdfs dfs -put -f mapper.py /user/$USER/mapper.py
```

Run the map reduce job again:
```
$ hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-streaming-2.7.3.2.6.3.0-235.jar -file mapper.py -mapper mapper.py -file reducer.py -reducer reducer.py -input /data/nyc/nyc-traffic.csv -output /user/$USER/output
```

The will put the final output in an 'output' dir as a part file.

To view this output file:
```
$ hadoop fs -cat output/part-00000
```

To view all the files in your user dir:
```
$ hadoop fs -ls
```

Before running again, remove the old output file and folder so it can be created again:
```
$ hadoop fs -rmr output
```

## Naive Solution

**reducer.py**

```
#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print '%s\t%s' % (current_word, current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%s' % (current_word, current_count)
```


**mapper.py**

```
#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(',')
    # increase counters
    for word in words[-5:]:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        if len(word) > 0: # dont do this fore the empty string
            print '%s\t%s' % (word, 1)

```

This does not check the *quality* of the data. So lines without the appropriate amount of fields or the header line will appear in the output.

**output/part-00000**
```
-74.0110059)"	1
"(40.6722269	1
"R/O 1 BEARD ST. ( IKEA'S	1
AMBULANCE	3713
BICYCLE	24153
BUS	25871
FIRE TRUCK	1333
LARGE COM VEH(6 OR MORE TIRES)	27981
LIVERY VEHICLE	17775
MOTORCYCLE	10029
OTHER	51360
PASSENGER VEHICLE	1005163
PEDICAB	123
PICK-UP TRUCK	26281
SCOOTER	534
SMALL COM VEH(4 TIRES)	30048
SPORT UTILITY / STATION WAGON	363210
TAXI	63892
UNKNOWN	105481
VAN	51666
VEHICLE TYPE CODE 1	1
VEHICLE TYPE CODE 2	1
VEHICLE TYPE CODE 3	1
VEHICLE TYPE CODE 4	1
VEHICLE TYPE CODE 5	1
```

## A Better Solution
The issue is we did not take into account the header row as well as rows that do not have all the desired columns. 

Now we can filter those out and we will get better results.

**reducer.py**

```
#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print '%s\t%s' % (current_word, current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%s' % (current_word, current_count)
```


**mapper.py**

```
#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(',')
    # increase counters
    if words[0] != "DATE" and len(words) > 27: 
        # do not get the header row and do not get it on a bad row with less then 27 fields
        for word in words[-5:]:
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            #
            # tab-delimited; the trivial word count is 1
            if len(word) > 0: # dont do this fore the empty string
                print '%s\t%s' % (word, 1)

```

New output:

**part-00000**

```
AMBULANCE	3713
BICYCLE	24153
BUS	25871
FIRE TRUCK	1333
LARGE COM VEH(6 OR MORE TIRES)	27981
LIVERY VEHICLE	17775
MOTORCYCLE	10029
OTHER	51360
PASSENGER VEHICLE	1005161
PEDICAB	123
PICK-UP TRUCK	26281
SCOOTER	534
SMALL COM VEH(4 TIRES)	30048
SPORT UTILITY / STATION WAGON	363210
TAXI	63892
UNKNOWN	105481
VAN	51666
```

## Final Logs

```
hornbd@hadoop-gate-0:~$ 
hornbd@hadoop-gate-0:~$ hadoop fs -put -f mapper.py /user/$USER/mapper.py
hornbd@hadoop-gate-0:~$ hadoop fs -put -f reducer.py /user/$USER/reducer.py
hornbd@hadoop-gate-0:~$ hadoop fs -cat output/part-00000
cat: `output/part-00000': No such file or directory
hornbd@hadoop-gate-0:~$ hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-streaming-2.7.3.2.6.3.0-235.jar -file mapper.py -mapper mapper.py -file reducer.py -reducer reducer.py -input /data/nyc/nyc-traffic.csv -output /user/$USER/output
18/04/02 16:30:07 WARN streaming.StreamJob: -file option is deprecated, please use generic option -files instead.
packageJobJar: [mapper.py, reducer.py] [/usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-streaming-2.7.3.2.6.3.0-235.jar] /tmp/streamjob967128608544033038.jar tmpDir=null
18/04/02 16:30:08 INFO client.RMProxy: Connecting to ResourceManager at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:8050
18/04/02 16:30:08 INFO client.AHSProxy: Connecting to Application History server at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:10200
18/04/02 16:30:08 INFO client.RMProxy: Connecting to ResourceManager at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:8050
18/04/02 16:30:08 INFO client.AHSProxy: Connecting to Application History server at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:10200
18/04/02 16:30:08 ERROR streaming.StreamJob: Error Launching job : Output directory hdfs://hadoop2-0-0.cscloud.ceas.uc.edu:8020/user/hornbd/output already exists
Streaming Command Failed!
hornbd@hadoop-gate-0:~$ hadoop fs -rmr output
rmr: DEPRECATED: Please use 'rm -r' instead.
18/04/02 16:31:17 INFO fs.TrashPolicyDefault: Moved: 'hdfs://hadoop2-0-0.cscloud.ceas.uc.edu:8020/user/hornbd/output' to trash at: hdfs://hadoop2-0-0.cscloud.ceas.uc.edu:8020/user/hornbd/.Trash/Current/user/hornbd/output1522701077193
hornbd@hadoop-gate-0:~$ hadoop jar /usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-streaming-2.7.3.2.6.3.0-235.jar -file mapper.py -mapper mapper.py -file reducer.py -reducer reducer.py -input /data/nyc/nyc-traffic.csv -output /user/$USER/output
18/04/02 16:31:20 WARN streaming.StreamJob: -file option is deprecated, please use generic option -files instead.
packageJobJar: [mapper.py, reducer.py] [/usr/hdp/2.6.3.0-235/hadoop-mapreduce/hadoop-streaming-2.7.3.2.6.3.0-235.jar] /tmp/streamjob2238521596560292967.jar tmpDir=null
18/04/02 16:31:21 INFO client.RMProxy: Connecting to ResourceManager at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:8050
18/04/02 16:31:21 INFO client.AHSProxy: Connecting to Application History server at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:10200
18/04/02 16:31:21 INFO client.RMProxy: Connecting to ResourceManager at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:8050
18/04/02 16:31:21 INFO client.AHSProxy: Connecting to Application History server at hadoop2-0-0.cscloud.ceas.uc.edu/192.168.2.20:10200
18/04/02 16:31:22 INFO mapred.FileInputFormat: Total input paths to process : 1
18/04/02 16:31:22 INFO net.NetworkTopology: Adding a new node: /default-rack/192.168.2.19:50010
18/04/02 16:31:22 INFO net.NetworkTopology: Adding a new node: /24-core/192.168.2.24:50010
18/04/02 16:31:22 INFO net.NetworkTopology: Adding a new node: /24-core/192.168.2.25:50010
18/04/02 16:31:22 INFO net.NetworkTopology: Adding a new node: /16-core/192.168.2.22:50010
18/04/02 16:31:22 INFO mapreduce.JobSubmitter: number of splits:2
18/04/02 16:31:22 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1521804639914_0159
18/04/02 16:31:22 INFO impl.YarnClientImpl: Submitted application application_1521804639914_0159
18/04/02 16:31:22 INFO mapreduce.Job: The url to track the job: http://hadoop2-0-0.cscloud.ceas.uc.edu:8088/proxy/application_1521804639914_0159/
18/04/02 16:31:22 INFO mapreduce.Job: Running job: job_1521804639914_0159
18/04/02 16:31:29 INFO mapreduce.Job: Job job_1521804639914_0159 running in uber mode : false
18/04/02 16:31:29 INFO mapreduce.Job:  map 0% reduce 0%
18/04/02 16:31:41 INFO mapreduce.Job:  map 100% reduce 0%
18/04/02 16:31:52 INFO mapreduce.Job:  map 100% reduce 99%
18/04/02 16:31:53 INFO mapreduce.Job:  map 100% reduce 100%
18/04/02 16:31:53 INFO mapreduce.Job: Job job_1521804639914_0159 completed successfully
18/04/02 16:31:53 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=40558930
		FILE: Number of bytes written=81577681
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=176307514
		HDFS: Number of bytes written=314
		HDFS: Number of read operations=9
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=2
		Launched reduce tasks=1
		Other local map tasks=2
		Total time spent by all maps in occupied slots (ms)=33708
		Total time spent by all reduces in occupied slots (ms)=18582
		Total time spent by all map tasks (ms)=16854
		Total time spent by all reduce tasks (ms)=9291
		Total vcore-milliseconds taken by all map tasks=16854
		Total vcore-milliseconds taken by all reduce tasks=9291
		Total megabyte-milliseconds taken by all map tasks=25887744
		Total megabyte-milliseconds taken by all reduce tasks=19027968
	Map-Reduce Framework
		Map input records=924098
		Map output records=1808611
		Map output bytes=36941702
		Map output materialized bytes=40558936
		Input split bytes=240
		Combine input records=0
		Combine output records=0
		Reduce input groups=17
		Reduce shuffle bytes=40558936
		Reduce input records=1808611
		Reduce output records=17
		Spilled Records=3617222
		Shuffled Maps =2
		Failed Shuffles=0
		Merged Map outputs=2
		GC time elapsed (ms)=497
		CPU time spent (ms)=20830
		Physical memory (bytes) snapshot=2696904704
		Virtual memory (bytes) snapshot=10287525888
		Total committed heap usage (bytes)=2602565632
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=176307274
	File Output Format Counters 
		Bytes Written=314
18/04/02 16:31:53 INFO streaming.StreamJob: Output directory: /user/hornbd/output
hornbd@hadoop-gate-0:~$ hadoop fs -cat output/part-00000
AMBULANCE	3713
BICYCLE	24153
BUS	25871
FIRE TRUCK	1333
LARGE COM VEH(6 OR MORE TIRES)	27981
LIVERY VEHICLE	17775
MOTORCYCLE	10029
OTHER	51360
PASSENGER VEHICLE	1005161
PEDICAB	123
PICK-UP TRUCK	26281
SCOOTER	534
SMALL COM VEH(4 TIRES)	30048
SPORT UTILITY / STATION WAGON	363210
TAXI	63892
UNKNOWN	105481
VAN	51666
hornbd@hadoop-gate-0:~$ 
```
