## Map Reduce Project

**Design Document of MapReduce project can be found in docs/.**

To run this project, simply follow these instructions:

### To run project and test scripts together::

Go to the main project directory and simply double click on the file: \
**DOUBLE_CLICK_TO_RUN_PROJECT.bat**

### To run individually::

>./gradlew run <br/>#this runs three UDFs on our implementation of the MapReduce Framework by starting the RunUDFs.java in start folder

To start the test script, use the following command:

> python test/testScript.py <br/>#runs three tests comparing output of map reduce for the three UDFs with actual expected output

### To run the implementation for your own UDF:

* Add the mapper function and reducer function in the **src/main/java/udf** folder
* Add config file with no of workers you need and the location of input/output/intermediate files in the **metadata** folder
* Add starter code in **start/runUDFs.java** file

## File Declarations:
* **MapReduce.java:** Contains the mapreduce interface that the test cases implement
* **Master.java:** This file contains the implementation of the master. This is where the workers are created, as well as the global synchronization barrier. For the mapper and reducer phases, we build separate processes.
* **Worker.java:** This file contains the implementation of the workers. Both file reading and writing operations are handled here.
* **WorkerHeartBeat.java:** Extends the thread class, that implements the heartbeat mechanism, allowing workers to send messages to the master.
* Test cases that implement this interface — **WordCount.java, URLFrequency.java, and DistributedGrep.java**

## The User Defined Functions (UDFs):
For MapReduce, the existing implementation supports multiple workers. It supports the following test
cases:
* **Word Count [WordCount.java]**: Count the frequency of each word in the user input file [input/wordcount_input.txt]
* **Distributed Grep [URLFrequency.java]**: Find the occurences of the word "distributed" in the user input file [input/distributedgrep_input.txt]
* **URL Frequency count [DistributedGrep.java]**: Count the frequency of each URL in the user input file [input/urlfrequency_input.txt]

## How the implementation works:
The following function declarations are part of the MapReduce interface that we implemented:
* The mapper function of each worker takes a row from the data partition allocated to that worker
and outputs a list of key value pairs, which are then written to intermediate files.
* The reducer function takes a list of values for a specific key and returns a string as the final result.
This values list is created after the combiner function is applied to the intermediate file data.
* The getSeparator function is used to obtain the separator that is used to separate rows in the input
for each test case.

## How the tests work:
The tests are started by running testScript.py present in the test folder. This file then runs
**wordCountTest.py**, **distributedGrepTest.py** and **URLFrequencyTest.py**. Both of these test files use
actual output to refer to the output of our MapReduce program, and expected output to refer to the
actual results used for verification. The respective test python files compute these predicted results. In
this way, we can see if our distributed task implementation produces the same results as a
non-distributed, single-process task implementation.
**Note**: Since the config files are located in the root directory of the project, the tests must be run from
there rather than from the test folder.

## Systems Specifications
Windows - 64 bit system
* Gradle 6.7 - the environment path variable also needs to be set
* JAVA (JDK 14) (Set the path of JDK in gradle.properties file)
* JAVA_HOME: path variable set in the environment variables.

License
----
MIT
