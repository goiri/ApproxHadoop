ApproxHadoop
============
Research has shown that approximate computing is effective at reducing the resource requirements, computation time, and/or energy consumption of large-scale computing. In this paper, we propose and evaluate a framework for creating and running approximation-enabled MapReduce programs.  Specifically, we propose approximation mechanisms that fit naturally into the MapReduce paradigm, including input data sampling, task dropping, and accepting and running a  precise and a user-defined approximate version of the MapReduce code. We then show how to leverage statistical theories to compute error bounds for popular classes of MapReduce programs when approximating with input data sampling and/or task dropping. We implement the proposed mechanisms and error bound estimations in a prototype system called ApproxHadoop.

Our evaluation uses MapReduce applications from different domains, including data analytics, scientific computing, video encoding, and machine learning.  Our results show that ApproxHadoop can significantly reduce application execution time and/or energy consumption when the user is willing to tolerate small errors. For example, ApproxHadoop can reduce runtimes by up to 32x when the user can tolerate an error of 1% with 95% confidence.  We conclude that our framework and system can make approximation easily accessible to many application domains using the MapReduce model. With this goal, we plan to make ApproxHadoop and our approximate programs publicly available.

Installation
------------
Get the sources for Hadoop (this version is tested with Hadoop 1.1.2):

    svn co http://svn.apache.org/repos/asf/hadoop/common/tags/release-1.1.2 hadoop-1.1.2-src
    cd hadoop-1.1.2-src/
    cd ant jar

Get the ApproxHadoop code:

    cd hadoop-1.1.2-src/src/conrib/
    git clone git@github.com:goiri/ApproxHadoop.git
    
Patch Hadoop with the approximation mechanisms:

    cd hadoop-1.1.2-src/
    patch -p0 -i src/contrib/ApproxHadoop/agileapproxhadoop2.patch
    
Compile ApproxHadoop:

    cd hadoop-1.1.2-src/src/contrib/ApproxHadoop
    ant jar

Usage
-----