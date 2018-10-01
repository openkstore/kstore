# K-Store

## Introduction

K-Store is a true column-oriented file format to speed up OLAP queries on large data sets.  Files created in K-Store format can be stored on the most popular file systems including Hadoop HDFS, Amazon S3, Azure Blob Storage, Google Cloud Storage or the local file system.

### Key Benefits

Faster than Parquet and ORC

(reading / writing operations)

### As flexible as JSON files

The schema is dynamic and is automatically discovered during inserts

### Cloud native

Optimized for cloud object storages

### File-system agnostic

Files created in K-Store format can be stored on the most popular file systems including Hadoop HDFS, Amazon S3, Azure Blob Storage, Google Cloud Storage or the local file system.

## Building

### Requirements

The K-Store project requires a JDK 8, Maven and Hadoop 2.x.x distribution to build modules.

### Environment variables

You need to have a $JAVA_HOME pointing on your JDK directory.

You need to have $HADOOP_HOME/bin in your $PATH, where $HADOOP_HOME refers to your Hadoop distribution. We use it to run test against HDFS.

#### Linux / Mac OS

There are no other requirements on those operating systems.

#### Windows

In your Hadoop distribution, you need to add hadoop.dll and winutils.exe in $HADOOP_HOME/bin. You can find the following files [here](https://github.com/steveloughran/winutils/tree/master/hadoop-2.8.3/bin). As Hadoop doesn’t support spaces in path, you shouldn’t have any spaces in your working environment.

### Maven commands

You can build the project by running: `mvn package`

## How to Contribute

We prefer to receive contributions in the form of GitHub pull requests. Please send pull requests against the kstore Git repository.

If you are looking for some ideas on what to contribute, please send a GitHub pull requests. Comment on the issue and/or contact contact@openkstore.org with your questions and ideas.

If you’d like to report a bug but don’t have time to fix it, email the mailing list GitHub pull requests

To contribute a patch:

1.  Break your work into small, single-purpose patches if possible. It’s much harder to merge in a large change with a lot of disjoint features.
    

2.  Submit the patch as a GitHub pull request against the master branch. For a tutorial, see the GitHub guides on forking a repo and sending a pull request.
    

3.  Make sure that your code passes the unit tests. You can run the tests with mvn test in the root directory.
    

4.  Add new unit tests for your code.
    

5.  We tend to do fairly close readings of pull requests, and you may get a lot of comments. Some common issues that are not code structure related, but still important:
    

Use tabs instead of whitespace.

Give your operators some room. Not a+b but a + b and not foo(int  a,int b) but foo(int a, int b).

Generally speaking, stick to the Sun Java Code Conventions

Make sure tests pass!

Thank you for getting involved!

### Authors and contributors

Pitton Olivier olivier@openkstore.org

Cournarie Eric  eric@openkstore.org

Dugé de Bernonville Rodolphe rodolphe@openkstore.org

### Code of Conduct

We hold ourselves and the K-Store developer community to the following code of conduct:

[The Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html)

### Discussions

Mailing list: contact@openkstore.org

Discussions also take place in github pull requests

### License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0