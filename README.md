# Spark Workshop

***
## Workshop Overview

With computer architectures across different fields scaling up to store large amounts of data, it is only natural that they also need to scale up to process this increasing size of data. This processing of big data ranges from simple SQL queries to applying machine learning algorithms on big data. Hadoop is the standard tool used for distributed computing across really large data sets. It has become ubiquitous with Big Data, providing a rich ecosystem of tools and techniques that allow users to utilize a large cluster of cheap hardware to do computing at supercomputer scale. MapReduce is a programming model and an associated implementation for processing and generating large data sets with a parallel, distributed algorithm on a cluster, and is used with Apache Hadoop. This technology has evolved over the last ten years and is widely used today. However, there are some well-known limitations associated with MapReduce, such that programming MapReduce jobs are notably difficult, as the Map and Reduce tasks need to be chained together in multiple steps for different applications. MapReduce also required data to be serialized to disk between each step, which means that the I/O cost of a MapReduce job is high, making interactive analysis and iterative algorithms (such as machine learning algorithms) very expensive.
To fix these problems, Hadoop has recently started moving towards a more general resource management framework for computation, YARN (Yet Another Resource Negotiator). YARN implements the next generation of MapReduce, but also allows applications to leverage distributed resources without having to compute with MapReduce.
Spark is the first fast, general purpose distributed computing paradigm that resulted from this shift and is gaining popularity rapidly. It was identified as one of the top evolving technologies as well as one of the top paying technologies by the Stack Overflow Development Survey. [1][2]
Spark extends the MapReduce model to support more types of computations using a functional programming paradigm, and it can cover a wide range of workflows that previously were implemented as specialized systems built on top of Hadoop. Spark uses in-memory caching to improve performance and, therefore, is fast enough to allow for interactive analysis and iterative algorithms, which greatly improve the performance for machine learning algorithms. The Spark Python API (PySpark) exposes the Spark programming model to Python, thus providing a great platform for developers to write spark jobs in Python. This will be the focus of our workshop. 

[[1]] (http://stackoverflow.com/research/developer-survey-2015)

[[2]] (http://stackoverflow.com/research/developer-survey-2016)

# So what will we do in this Spark Workshop?

First, check out the instructions to set up Spark/PySpark on your local machine.

https://github.com/vikaasa/Spark_Workshop/wiki/Spark-Installation

This will make it easier for us to get started right away on the day of the workshop!

We will begin by providing a brief introduction to Spark, RDDs, and the MapReduce programming paradigm, and its relevance in processing big data today. 

We then move on to actually working with a sample dataset in PySpark, by performing simple text manipulation and computing TF-IDF scores on the dataset. We follow that up by exploring a simple classification problem using the Random Forests Machine Learning algorithm on PySpark. 

