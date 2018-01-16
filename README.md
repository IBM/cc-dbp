# cc-dbp

A dataset for knowledge base population research using Common Crawl and DBpedia.

For a quick introduction see [configSmall.properties](configSmall.properties) and [createSmall.sh](createSmall.sh). This will download 1/80th of the December 2017 Common Crawl and create a KBP dataset from it.

The parameter warcFileLimit in configSmall.properties limits the Common Crawl dataset to 1/80th (1000 of 80000 files). If this parameter is changed then the maxNodeCorpusCount parameter should probably also be changed by a proportional amount.

The warcUrlList variable in createSmall.sh selects the particular version of Common Crawl. New versions are announced on the [Common Crawl blog](http://commoncrawl.org/connect/blog/). The URL should be the link to the WARC files.


The createSmall.sh script proceeds in steps

  1. Downloads the DBpedia files and creates an initial subset using the [selected relations](com.ibm.research.ai.ki.kb/src/main/resources/relationSample.txt)
  1. Downloads Common Crawl and filters it to text documents in the selected langauge
  1. Finds the counts in the corpus of each label in the subset of DBpedia
  1. Finalizes construction of the DBpedia subset, including filtering out nodes whose labels are too frequent
  1. Annotates Common Crawl with the node mentions of the knowledge base - found by gazetteer matching
  1. Constructs context sets where two nodes occur in a sentence together

The dataset can also be built using Spark. For this option:

  1. Run [ConvertDBpedia](com.ibm.research.ai.ki.kb/src/main/java/com/ibm/research/ai/ki/kb/conversion/ConvertDBpedia.java) as in non-Spark version.
  1. Use [SaveCommonCrawlHdfs](com.ibm.research.ai.ki.corpus/src/main/java/com/ibm/research/ai/ki/corpora/crawl/SaveCommonCrawlHdfs.java) (rather than SaveCommonCrawl) 
  1. Use [DocEntityStats](com.ibm.research.ai.ki.spark/src/main/java/com/ibm/research/ai/ki/spark/DocEntityStats.java) rather than GazetteerEDL to get node corpus counts. 
  1. Again run [ConvertDBpedia](com.ibm.research.ai.ki.kb/src/main/java/com/ibm/research/ai/ki/kb/conversion/ConvertDBpedia.java) as in non-Spark version.
  1. Use [GazetteerPreprocess](com.ibm.research.ai.ki.spark/src/main/java/com/ibm/research/ai/ki/spark/GazetteerPreprocess.java) rather than GazetteerEDL to create the annotated Common Crawl. 
  1. Finally, use [RelexBuildDataset](com.ibm.research.ai.ki.spark/src/main/java/com/ibm/research/ai/ki/spark/RelexBuildDataset.java) rather than KBPBuildDataset to create the context sets.

