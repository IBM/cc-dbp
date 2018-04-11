#!/bin/bash

#stop at first error, unset variables are errors
set -o nounset
set -o errexit

# Base directory to save the cc-dbp dataset in
baseDir=$1

#CONSIDER: take from command line
config=config.properties
warcUrlList=https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-51/warc.paths.gz

mvn clean compile package

cd com.ibm.research.ai.ki.kb
mvn assembly:single
cd ..
cd com.ibm.research.ai.ki.corpus
mvn assembly:single
cd ..
cd com.ibm.research.ai.ki.kbp
mvn assembly:single
cd ..

# Download DBpedia files and create initial kb files
java -Xmx4G -cp com.ibm.research.ai.ki.kb/target/kb-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.ibm.research.ai.ki.kb.conversion.ConvertDBpedia -config $config -kb $baseDir/kb

# Download Common Crawl, get -urlList from http://commoncrawl.org/connect/blog/
java -Xmx4G -cp com.ibm.research.ai.ki.corpus/target/corpora-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.ibm.research.ai.ki.corpora.crawl.SaveCommonCrawl -config $config -urlList $warcUrlList -out $baseDir/docs.json.gz.b64

# get node corpus counts by baseline EDL
java -Xmx4G -cp com.ibm.research.ai.ki.kbp/target/kbp-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.ibm.research.ai.ki.kbp.GazetteerEDL $baseDir/kb/gazEntries.ser.gz $baseDir/docs.json.gz.b64 $baseDir/docs-gaz.json.gz.b64 $baseDir/kb/idCounts.tsv

# create remaining KB files
java -Xmx4G -cp com.ibm.research.ai.ki.kb/target/kb-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.ibm.research.ai.ki.kb.conversion.ConvertDBpedia -config $config -kb $baseDir/kb

# annotate corpus with baseline EDL
java -Xmx4G -cp com.ibm.research.ai.ki.kbp/target/kbp-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.ibm.research.ai.ki.kbp.GazetteerEDL $baseDir/kb/gazEntriesFiltered.ser.gz $baseDir/docs.json.gz.b64 $baseDir/docs-gaz.json.gz.b64

# baseline context set construction
java -Xmx4G -cp com.ibm.research.ai.ki.kbp/target/kbp-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.ibm.research.ai.ki.kbp.KBPBuildDataset -config $config -in $baseDir/docs-gaz.json.gz.b64 -out $baseDir/dataset -kb $baseDir/kb

# show sample of positive context sets
awk  -F $'\t' '$7!=""' $baseDir/dataset/contextSets/contexts-part0.tsv | head
