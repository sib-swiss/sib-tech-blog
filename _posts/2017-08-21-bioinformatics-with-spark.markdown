---
layout: post
title:  "Tutorial on Spark for Bioinformatics"
date:   2017-08-10 19:11:27 +0200
categories: spark analytics
---

# DRAFT DOCUMENT (NOT READY FOR PUBLISH)

This tutorial shows how to analyse protein sequences and amino acids going through the basics of Apache Spark. The same can be applied to genomic data with nucletidics bases.
Look at ADAM project for more complex examples.

# Download data

The procedure is intended to be run on standalone in a laptop, but it can also be run in cluster mode as well.
Download the [swissprot-aug-2017.tsv](ftp://swissprot) (209MB) that contains all SwissProt entries from August 2017 release. 

Each line corresponds to an entry and is shown in the following format
{% highlight tsv %}
Accession Gene  Specie  Sequence 
P01308  INS HUMAN AJBFSAKLBASFKJ
{% endhighlight %}

# Setup Spark environment
You can use this tutorial with spark-shell or spark notebooks:

- **spark-shell**: Download [Apache Spark](https://spark.apache.org/) and start a spark shell `$SPARK_HOME/bin/spark-shell`. 
  - By default spark uses all CPUs available in the machine. But you can use `$SPARK_HOME/bin/spark-shell master[2]` if you would like to use only 2 cores. In case you would like to run in a cluster specify the `$SPARK_HOME/bin/spark-shell spark://....`
- **spark-notebook**: Download a spark-notebook version, like for example: [spark-notebook 0.7.0](https://s3.eu-central-1.amazonaws.com/spark-notebook/tgz/spark-notebook-0.7.0-pre2-scala-2.11.7-spark-1.6.2-hadoop-2.7.3-with-hive-with-parquet.tgz?max-keys=100000) from [http://spark-notebook.io/](http://spark-notebook.io/). Extract the archive and start the notebook with the command `./bin/spark-notebook` and access the interface at [http://localhost:9001/](http://localhost:9001/).

# Procedure 

#### Read the data and get to know RDDs

{% highlight scala %}
//Read data first
val data = sc.textFile("swissprot.tsv")
/**data: org.apache.spark.rdd.RDD[String] = swissprot.tsv MapPartitionsRDD[36] at textFile...**/
{% endhighlight %}

Data can be read from directory using wildcards and can even compressed. For example you can write `sc.textFile("apache-logs/**/*.gz")`. You can also read from HDFS, ...

Note that this operation goes pretty fast and it will even work if the file is not present. Note that if you run on a cluster the file must be accessed in all workers.
To make sure we have placed the file in the correct directory, show the first line: 
RDD stands for Resiliant Data Set and are ....

{% highlight scala %}
//Shows the first line
data.first 
{% endhighlight %}

If you get an error message that says "Input path does not exist:" then you might need to specify the full path of the file (if you didn't start the shell where the file was)

## Classes and functions

Even though we can use anonymous functions in lambda, it is easier to understand the code if we use functions and classes.

{% highlight scala %}
//Defines a class to represent the data in TSV 
case class Sequence(accession: String, geneName: String, specie: String, sequence: String)

//Defines a function that reads a line from the TSV file and returns an object of Sequence class
def readLine(line: String) = {
  val parts = line.split("\t"); 
  Sequence(parts(0), parts(1), parts(2), parts(3))
}
{% endhighlight %}

## Transformations and actions

In Spark there are 2 concepts. Transformations and actions.


{% highlight scala %}
//This is a transfromation that tells how the lines should be read (nothing is executed)
val sequences = data.map(readLine)

//Do we need this? sequences.take(1)
//sequences.cache //Check if enough memory! 

//Shows first accession (this is an action, but only first row access)
sequences.first.accession
/* sysout: String = Q6GZX4 */
sequences.first.specie

sequences.distinct species

// This is an action, but all rows must be accessed, because file is not indexed yet.
val humanSequences = sequences.filter(s=> s.specie.equals("HUMAN"))

//Count the number of human sequences
humanSequences.count
/* sysout: Long = 20214 */
{% endhighlight %}


You will note that this operation also goes pretty fast, this is because only the first line is actually read and not the all file!

## Cache flatMap and reduceByKey

{% highlight scala %}
humanSequences.cache

//Show how many entries are here (Job is dispatched!!)
humanSequences.count

//Show histogram by specie (note the L to make sure we are using Longs instead of Integers here!)
val aaFrequency = humanSequences.flatMap(s=> s.sequence.toList).map(aa => (aa, 1L)).reduceByKey(_ + _)

//Sort
aaFrequency.collect
aaFrequency.sortBy(-_._2).collect
{% endhighlight %}

![AA frequency]({{ site.url }}/sib-tech-blog/assets/aaFrequency.png)

## DataFrame and SQL

{% highlight scala %}

//Use data frames
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

{% endhighlight %}


## File format(s)
Avro and Parquet

{% highlight scala %}

//File
//out.csv (200MB)

//Zipped(66M)

//Save into format (109 MB) du -h
sequences.toDF.write.format("parquet").save("sequences-no-part.parquet")

//If you want to manipulate by specie
sequences.toDF.write.partitionBy("specie").format("parquet").save("sequences.parquet")
{% endhighlight %}

# Appendix
[python-script]: #python-script
Since from [uniprot] website we could only find fasta, we used the  [python-script][python-script] below to convert from FASTA to tsv.

<a name="python-script"></a>
#### FASTA to TSV converter in python
{% highlight python %}
import sys
from Bio import SeqIO
fasta_file = sys.argv[1]
for seq_record in SeqIO.parse(fasta_file, "fasta"):
    header = str(seq_record.id).split('|')
    accessionSpecie = str(header[2]).split("_")
    sys.stdout.write(str(header[1])+'\t'+str(accessionSpecie[0])+'\t'+str(accessionSpecie[1])+'\t'+str(seq_record.seq)+ '\n')
{% endhighlight %}

[jekyll-docs]: https://jekyllrb.com/docs/home
[jekyll-gh]:   https://github.com/jekyll/jekyll
[jekyll-talk]: https://talk.jekyllrb.com/

This post was written by [Daniel Teixeira](http://github.com/ddtxra).
