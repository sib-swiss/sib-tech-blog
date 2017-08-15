---
layout: post
title:  "Tutorial on Spark for Bioinformatics"
date:   2017-08-10 19:11:27 +0200
categories: spark analytics
---

<style>
table{
  margin: auto;
  width: 50%;
}
th, td {
	border: 1px solid lightgrey;
  padding-left: 10px;
  padding-right: 10px;
}
</style>

# DRAFT DOCUMENT (NOT READY FOR PUBLISH)

This tutorial gives an introduction to Apache Spark taking as use case protein sequences and amino acids, commonly used in  bioinformatics. The same procedure can also be applied to  genomic data with nucleotides (A,C,G,T).

# Download data

The dataset used for this tutorial corresponds to all protein sequences manually reviewed by SwissProt until August 2017 [swissprot-aug-2017.tsv.gz](ftp://ftp.vital-it.ch/tools/sib-tech-blog/spark-for-bioinformatics/swissprot-aug-2017.tsv.gz) (66.0 MB compressed, 200 MB uncompressed, 555'100 sequences or lines). 
This data corresponds to the [Swiss-Prot](http://www.uniprot.org/uniprot/?query=*&fil=reviewed%3Ayes) data (protein sequences manually reviewed by curators).
The data size is good enough to teach the basics of Spark and run on a laptop in standalone.

There is also a more challenging dataset to run / benchmark on a cluster, that can be downloaded through this link [uniprot-trembl-aug-2017.tsv.gz](ftp://ftp.vital-it.ch/tools/sib-tech-blog/spark-for-bioinformatics/uniprot-trembl-aug-2017.tsv.gz) (16.7GB compressed, 29.7GB uncompressed, 88'032'926 sequences or lines). This data corresponds to the [TrEMBL](http://www.uniprot.org/uniprot/?query=*&fil=reviewed%3Ano) data that was not reviewed by curators.

Each line corresponds to an entry and is shown in the following format
{% highlight tsv %}
Accession Gene  Specie  Sequence 
P01308  INS HUMAN AJBFSAKLBASFKJ
{% endhighlight %}

# Setup Spark environment
You can use this tutorial with spark-shell or spark notebooks:

- **spark-shell**: Download [Apache Spark](https://spark.apache.org/) and start a spark shell `$SPARK_HOME/bin/spark-shell --driver-memory 2G`. 
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

In Spark there are two type of operations: Transformations and actions.

- Transformations (transform) create a new RDD from an existing one. They are **lazily executed** ! (They are executed on demand)

- An action will return a non-RDD type. Actions triggers execution and usually CPU time.

<br>

| Transformations | Actions |
|-----------------|---------|
| map             | first, take    |
| filter          | collect |
| distinct        | count   |
| ...      | ...   |

<br>
Let's see some examples:

{% highlight scala %}
//In this case map tells how each line should be mapped / transformed (nothing is actually executed)
val sequences = data.map(readLine)

//Shows first accession or specie (this is an action, but does not require to read the whole file)
sequences.first
sequences.first.getClass //Notice that this is a class and not an RDD

sequences.first.accession
sequences.first.specie

//If you have initialized the spark-shell with 2G you may be able to store the result in cache (big difference with Hadoop)
sequences.cache

//See how many different species we have
sequences.map(s => s.specie).distinct().count

//See the top curated species
sequences.map(s => (s.specie, 1)).reduceByKey(_ + _).sortBy(-_._2).take(10).foreach(println)

// This is an action, but all rows must be accessed, because file is not indexed yet.
val humanSequences = sequences.filter(s=> s.specie.equals("HUMAN"))
//Count the number of human sequences
humanSequences.count

//Sequences containing the 'DANIEL' sequence 
val sequencesContainingPattern = sequences.filter(s=> s.sequence.contains("DANIEL")).count

//Use any function (alignment, GC content, ..... ) 
val result = sequences.map(ANY_FUNCTION_HERE)

//example of custom functions
//Need to add --packages at the end (corresponding to Java/Scala artifact. Downloaded by sbt automatically and deployed to worker nodes.
 $SPARK_HOME/bin/spark-shell --master spark://$master_hostname:7077 --packages "com.fulcrumgenomics:fgbio_2.11:0.2.0"

import com.fulcrumgenomics.alignment.NeedlemanWunschAligner

 val seqToAlign = "MALWMRLLPLLALLALWGPDPAAAFVNQHLCGSHLVEALYLVCGERGFFYTPKTRREAEDLQVGQVELGGGPGAGSLQPLALEGSLQKRGIVEQCCTSICSLYQLENYCD"

humanSequences.map(s=> (s.accession, NeedlemanWunschAligner(1, -1, -3, -1).align(seqToAlign, s.sequence).score)).sortBy(-_._2).take(10).foreach(println)

{% endhighlight %}

## Spark notebook

Spark notebooks offer the possibilty to visualize directly results in a chart.

{% highlight scala %}
val sequences = data.map(readLine)
val humanSequences = sequences.filter(s=> s.specie.equals("HUMAN"))
val aaFrequency = humanSequences.flatMap(s=> s.sequence.toList).map(aa => (aa, 1L)).reduceByKey(_ + _).sortBy(-_._2)

//The collect or take action will trigger the chart
aaFrequency.collect
{% endhighlight %}

![AA frequency]({{ site.url }}/sib-tech-blog/assets/aaFrequency.png)

## DataFrame and SQL

{% highlight scala %}

//Use data frames
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val df = sequences.toDF
df.show
// Print the schema in a tree format (you can imagine nested classes inside sequence)
df.printSchema()

df.createOrReplaceTempView("sequences")

val sqlQuery = """SELECT specie, count(*) as cnt 
FROM sequences 
GROUP BY specie 
ORDER BY cnt DESC 
LIMIT 10"""

sqlContext.sql(sqlQuery).show

//OR directly 
df.groupBy("specie").agg(count("*") as "cnt").orderBy(desc("cnt")).limit(10).show

//Or a more complex example, that also computes the average of length of the sequence
This query sorts by the average lenght, you can see the sequence with less amino acids:
//http://www.uniprot.org/uniprot/P84761

val sqlQuery2 =
"""val sqlQuery = "SELECT specie, count(*) as cnt, avg(length(sequence)) as avgSeqLength FROM sequences GROUP BY specie ORDER BY avgSeqLength ASC LIMIT 10"""

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


### ADAM project
ADAM is a genomics analysis platform with specialized file formats built using Apache Avro, Apache Spark and Parquet. Apache 2 licensed. 

Look at [ADAM](https://github.com/bigdatagenomics/adam) project for more complex examples.

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
