# LDBC SNB GraphDB implementation

This directory contains the [GraphDB](https://www.ontotext.com/products/graphdb/) implementation of the Interactive workload of the [LDBC SNB benchmark](https://github.com/ldbc/ldbc_snb_docs).

## Setup

The recommended environment for executing this benchmark is as follows: the benchmark scripts (Bash) and the LDBC driver (Java 8) run on the host machine. Therefore, the requirements are as follows:

* Bash
* Java 8
* Python version 2.7.X

## Generating and loading the data set

### Generating LDBC SNB Dataset representing Social Network

To generate benchmark data, we need to have Hadoop on top of which LDBC SNB Datagen runs. The latter could be downloaded from [here](http://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz).
After that, download [Hadoop-based Datagen](https://github.com/ldbc/ldbc_snb_datagen_hadoop) and after it is decompressed, you will have a folder called ***“ldbc_snb_datagen-<downloaded_version>”***.

Datagen provides a run.sh which is a script to automate its compilation and execution. It needs to be configured for your environment, so open it and set the two variables at the top of the script to the corresponding paths.

```
HADOOP_HOME=/<path_to_hadoop_dir>/hadoop-3.2.1
LDBC_SNB_DATAGEN_HOME=/<path_to_dir>/ldbc_snb_datagen
```

HADOOP_HOME points to the path where hadoop-3.2.1 is installed, while LDBC_SNB_DATAGEN_HOME points to where Hadoop-based Datagen is installed.

In order to create dataset in desired .ttl format user should provide properly configured ***params.ini*** file in /<path_to_dir>/ldbc_snb_datagen-<datagen_version> directory.

```
ldbc.snb.datagen.generator.scaleFactor:snb.interactive.1                                                                              
ldbc.snb.datagen.generator.numPersons:1000                                                                                          
ldbc.snb.datagen.generator.maxNumFriends:30                                                                                          
ldbc.snb.datagen.generator.maxNumTagsPerUser:20                                                                                   
ldbc.snb.datagen.generator.numYears:3                                                                                         
ldbc.snb.datagen.generator.startYear:2010                                                                                          
ldbc.snb.datagen.generator.blockSize:30                                                                                            
                                                                                                                                        
ldbc.snb.datagen.serializer.personSerializer:ldbc.snb.datagen.serializer.snb.interactive.TurtlePersonSerializer               
ldbc.snb.datagen.serializer.invariantSerializer:ldbc.snb.datagen.serializer.snb.interactive.TurtleInvariantSerializer              
ldbc.snb.datagen.serializer.personActivitySerializer:ldbc.snb.datagen.serializer.snb.interactive.TurtlePersonActivitySerializer
ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.turtle.TurtleDynamicActivitySerializer    
ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.turtle.TurtleDynamicPersonSerializer     
ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.turtle.TurtleStaticSerializer                        

ldbc.snb.datagen.serializer.outputDir:/<path_to_directory_where_ttl_files_to_be_stored>
```
The snb.interactive.1 scale factor will create a dataset of approximately 47 million statements. One could modify the number of persons, friends, etc parameters.

Now, you can execute run.sh script to compile and execute Datagen using the provided params.ini file. Type the following commands:
```bash
$ cd /<path_to_dir>/ldbc_snb_datagen-<datagen_version>
$ ./run.sh
```
The result of execution will generate three directories in the provided outputDir: hadoop, social_network and substitution_parameters.The last one will be empty at this stage.

In order to generate query substitution parameters, you should execute
```bash
$ cd /<path_to_dir>/ldbc_snb_datagen-<datagen_version>/paramgenerator
$ ./generateparams.py /<path_to_directory_where_ttl_files_to_be_stored>/hadoop /<path_to_directory_where_ttl_files_to_be_stored>/substitution_parameters.
```
Afterwards you should upload created ***social_network_activity_0_0.ttl, social_network_person_0_0.ttl*** and ***social_network_static_0_0.ttl*** files from /<path_to_directory_where_ttl_files_to_be_stored>/social_network/ folder into running GraphDB repository.

## Running the benchmark

Checkout https://github.com/Ontotext-AD/ldbc_snb_driver fork and build branch GDB-5779-Benchmark_Performance_of_Property_Path_Search 
```bash
$ mvn clean install -DskipTests
```

Create ***interactive-benchmark.properties*** file with following content:

```
endpoint=http://<host>:<port>/repositories/<repo_id>
user=admin # optional if the endpoint is secured
password=root # optional if the endpoint is secured
queryDir=<path_to_benchmark_queries>
printQueryNames=true # configurable
printQueryStrings=true # configurable
printQueryResults=true # configurable
#cvp=<path_to_valid_file_where_validation_parameters_will_be_generated>|100 # uncomment this line if you’d like to create validation parameters
#vdb=<path_to_valid_file_where_validation_parameters_are_located> # uncomment this line if you’d like to run the benchmark in validation mode. Note that first you should generate validation parameters.

status=1
thread_count=1
name=LDBC-SNB
results_log=true
flush_log=true
time_unit=MILLISECONDS
time_compression_ratio=0.001
peer_identifiers=
workload_statistics=false
spinner_wait_duration=1
help=false
ignore_scheduled_start_times=true

workload=com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.LdbcSnbInteractiveGraphDBWorkload 
db=com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.LdbcSnbInteractiveGraphDb
operation_count=250
ldbc.snb.interactive.parameters_dir=<path_to>/substitution_parameters/
ldbc.snb.interactive.short_read_dissipation=0.2
## The ldbc.snb.interactive.update_interleave driver parameter must come from the
## updateStream.properties file, which is created by the data generator.
## This parameter should NEVER be set manually.
ldbc.snb.interactive.update_interleave=4477

warmup=100

## frequency of read queries (number of update queries per one read query)
## Make sure that the frequencies are those for the selected scale factor
## as found on section B.1 "Scale Factor Statistics for the Interactive workload"
## at http://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf
ldbc.snb.interactive.LdbcQuery1_freq=26
ldbc.snb.interactive.LdbcQuery2_freq=37
ldbc.snb.interactive.LdbcQuery3_freq=69
ldbc.snb.interactive.LdbcQuery4_freq=36
ldbc.snb.interactive.LdbcQuery5_freq=57
ldbc.snb.interactive.LdbcQuery6_freq=129
ldbc.snb.interactive.LdbcQuery7_freq=87
ldbc.snb.interactive.LdbcQuery8_freq=45
ldbc.snb.interactive.LdbcQuery9_freq=157
ldbc.snb.interactive.LdbcQuery10_freq=30
ldbc.snb.interactive.LdbcQuery11_freq=16
ldbc.snb.interactive.LdbcQuery12_freq=44
ldbc.snb.interactive.LdbcQuery13_freq=19
ldbc.snb.interactive.LdbcQuery14_freq=49

# *** For debugging purposes ***
# *** Allows to exclude given queries ***

ldbc.snb.interactive.LdbcQuery1_enable=true
ldbc.snb.interactive.LdbcQuery2_enable=true
ldbc.snb.interactive.LdbcQuery3_enable=true
ldbc.snb.interactive.LdbcQuery4_enable=true
ldbc.snb.interactive.LdbcQuery5_enable=true
ldbc.snb.interactive.LdbcQuery6_enable=true
ldbc.snb.interactive.LdbcQuery7_enable=true
ldbc.snb.interactive.LdbcQuery8_enable=true
ldbc.snb.interactive.LdbcQuery9_enable=true
ldbc.snb.interactive.LdbcQuery10_enable=true
ldbc.snb.interactive.LdbcQuery11_enable=true
ldbc.snb.interactive.LdbcQuery12_enable=true
ldbc.snb.interactive.LdbcQuery13_enable=true
ldbc.snb.interactive.LdbcQuery14_enable=false
```

***Go to target directory of the project and execute:***

```bash
$ java -cp jeeves-standalone.jar com.ldbc.driver.Client -P /<path_to>/interactive-benchmark.properties
```