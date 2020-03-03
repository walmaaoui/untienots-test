# UntieNots technical test

This is a solution to a data engineering technical test from untienots

## Installation

To install the environment (zookeeper and kafka), use [docker-compose](https://docs.docker.com/compose/) once in the project folder.

```bash
docker-compose up -d
```

## Usage
The project contains 4 sub-modules

### file-to-kafka
A Spark batch that reads data from several files in a folder,
split their contents into words and send them to a kafka topic in the format: 
`{"source":"file_name","word":"abc"}`

To run the job

```bash
sbt "project file-to-kafka" "run --arg1 value1 --arg2 value2"
```

`file-to-kafka` accepts the following arguments:  

```bash
$ sbt "project file-to-kafka" "run --help" 

file-to-kafka 0.1
Usage: file-to-kafka [options]

  --name <value>           App name, will be used also as the spark app name
  --folder-path <value>    Glob path to input files
  --topic <value>          Kafka topic to which data will be sent
  --kafka-broker <value>   Kafka broker host:port to which data will be sent
  --to-lower-case <value>  Boolean, whether or not to transform input text to lower case
  --remove-punctuation <value>
                           Boolean, whether or not to remove punctuation from input text
  --remove-common-words <value>
                           Boolean, whether or not to remove common words
  --help                   prints this usage text
```

Note that all arguments are optional and default values are defined in the file `application.conf specific to this module. 

```javascript
app {
  name = "file-to-kafka"
  default-folder-path = "file-to-kafka/src/main/resources/data/*"
  default-topic = "words"
  file-processing {
    to-lower-case = false
    remove-punctuation = false
    remove-common-words = false
  }
}

kafka {
  host = "localhost:29092"
}
```

The above applies to all other modules.

To run tests

```bash
sbt "project file-to-kafka" test
```

Once the job was run, you can consume data from the topic to see it, you can use a cli consumer or desktop client, 
I recommend [conduktor](https://www.conduktor.io/)
broker host: `localhost:29092`

![Alt text](/screenshots/topic-words.png?raw=true "topic-words")


### words-stream-processing
A Spark streaming application that has as a configuration a list of topics that should be monitored, 
a topic has a name and a list of keywords, ex

```javascript
topics {
    color = ["red", "blue", "green"]
    sport = ["football", "tennis", "horseriding", "striker"]
    plane = ["wing", "pilot", "propeller", "striker"]
  }
```

This application consumes the previous topic and 
- If the word is in the keyword list for one or several topics, send a message in another queue
Q2: `{"source": <file_name >, "word": <word>, "topics": [<topics>] }`
- If the word corresponds to a topic name, send in a third queue Q3: `{"source": <file_name>,
"topic": <topic>}`

To run the job

```bash
sbt "project words-stream-processing" "run --arg1 value1 --arg2 value2"
```

words-stream-processing accepts the following arguments:  

```bash
$ sbt "project words-stream-processing" "run --help" 

words-stream-processing 0.1
Usage: words-stream-processing [options]

  --name <value>           App name, will be used also as the spark app name
  --words-topic <value>    Kafka words topic which has the input stream
  --words-topic <value>    Kafka words topic from which the words stream will be read
  --words-topics-topic <value>
                           Kafka topic to which topics by word will be sent
  --files-topics-topic <value>
                           Kafka topic to which topic by file will be sent
  --help                   prints this usage text
```

To run tests

```bash
sbt "project words-stream-processing" test
```

![Alt text](/screenshots/file-topic.png?raw=true "file-topic")

![Alt text](/screenshots/file-word-topics.png?raw=true "file-words-topics")


### kafka-to-parquet
A Spark Batch application that reads Q2 and Q3 and write them to parquet files


To run the job

```bash
sbt "project kafka-to-parquet" "run --arg1 value1 --arg2 value2"
```

kafka-to-parquet accepts the following arguments:  

```bash
$ sbt "project kafka-to-parquet" "run --help" 

kafka-to-parquet 0.1
Usage: kafka-to-parquet [options]

  --name <value>          App name, will be used also as the spark app name
  --input-topics <value>  Topics to copy to parquet
  --help                  prints this usage text
```

To run tests

```bash
sbt "project kafka-to-parquet" test
```

![Alt text](/screenshots/files-parquet.png?raw=true "files-parquet")


### analytics
A Spark Batch application that reads the parquet file from (Q2) 
and compute for each topic\theme:
- the sources associated with the number of occurrences for each key word.
- the false positives (sources identified with the keywords that do not belong to the topic) =>
We assume that a source belongs to a topic if X% of its keywords can be found in the source.
(X is an argument of the script).
- the relevance of each keyword: rate of presence in a source belonging to the topic/ rate of
presence in a source not belonging to the topic / rate of absence in a source that belonged to
the topic

check unit tests for examples. 

To run the job

```bash
sbt "project analytics" "run --arg1 value1 --arg2 value2"
```

kafka-to-parquet accepts the following arguments:  

```bash
$ sbt "project analytics" "run --help" 

analytics 0.1
Usage: analytics [options]

  --name <value>           App name, will be used also as the spark app name
  --analytics-path <value>
                           Path to file-topic data
  --word-topics-path <value>
                           Path to word-topics data
  --belonging-threshold <value>
                           Kafka broker host:port to which data will be sent
  --help                   prints this usage text
```

To run tests

```bash
sbt "project analytics" test
```

This is the output of running all the pipeline on the data in `file-to-kafka/resources/data`. 
This shows the final result and the dataset schemas. However the values are not very significant, 
check the unit tests for better examples. 

![Alt text](/screenshots/analytics-output.png?raw=true "analytics-output")


