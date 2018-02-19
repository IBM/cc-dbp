# cc-dbp

A dataset for knowledge base population research using Common Crawl and DBpedia.

### Quickstart

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

### Multi-lingual Support
  
The dataset construction also supports using any language from DBpedia. See configSmall-de.properties, changes from configSmall.properties are commented with "German language". The script createSmall-de.sh is the same as createSmall.sh, it just points to the different proeperties file.

## JSON Document Format

### From Common-Crawl + Preprocessing (Pre-EDL)

The documents from Common Crawl are saved one per line in base64 encoded gzipped JSON. To see the text of a JSON document, select a line, base64 decode and gunzip it.
This command gives the tenth document in part-0:
```
(hdfs dfs -)cat /common-crawl/oct2017-docs.json.gz.b64/part-0 | head -10 | tail -1 | base64 --decode | gunzip
```

An example of the JSON format is given below. The id of the document is the URL from the WARC file. Then the text of the document is given, this is the result of processing the html with boilerpipe. 

The annotations of the document are spans of text with some meaning attached, they are organized by type. The only annotations in the original documents by default are the LinkAnnotations. Each annotation has an 'id' and an 'anno' field, that gives the content of the annotation. The 'id' field exists so that annotations can refer to other annotations (such as a dependency parse). The 'anno' part will always contain 'start' and 'end' character offsets. In the case of LinkAnnotation, it also contains the target of the link. 

The 'structures' contain metadata that applies to the document as a whole, rather than any particular span. The only document structure is the 'source' which gives the WARC file that the document is from.

```json
{"id":"http://101apartmentforrent.com/apartments/nh_others_apartment.html",
"text":"Apartments for Rent in New Hampshire: apartment rental directory and apartment finder\n\nHome\nGuide\nApartments\nMovers\nAgencies\nAbout\n\nApartments for Rent in New Hampshire\n\n< prev1 2 3 4 next >\n\nAddress: 232 Suncook Valley Road\n\nAlton, NH 03809\nPhone: (603) 875-6313\n\nHelp-U-Sell Ken Gogan Realty\nAddress: 89 Route 101a\n\nAmherst, NH 03031\nPhone: (603) 816-2790\n\n» More Info\n...",
"annotations":{"LinkAnnotation":[{"id":0,"anno":{"start":87,"end":91,"target":"/"}},{"id":1,"anno":{"start":92,"end":97,"target":"/guide/"}},...]},
"structures":{"DocumentSource":{"source":"crawl-data/CC-MAIN-2017-43/segments/1508187820466.2/warc/CC-MAIN-20171016214209-20171016234209-00001.warc.gz"}}}
```

### After Entity Detection and Linking

After Entity Detection and Linking (EDL) the documents contain Paragraph, Sentence, Token and EntityWithId annotations. Any annotations from the HTML are dropped. Not shown in the example, the annotations also have a 'source' field that gives the name of the annotator that produced them, such as OpenNLPSentence or GazetteerMatcher. Paragraph, Sentence and Token annotations only provide the span of the annotation while EntityWithId gives the type and the node ID as well. The paragraph, sentence and token annotations are used during context set construction to provide units of context, which could be paragraphs, sentences or token windows.

```
(hdfs dfs -)cat /common-crawl/dbp/oct2017-docs-gaz.json.gz.b64/part-00090  | head -10 | tail -1 | base64 --decode | gunzip
```

```json
{"id":"http://1019ampradio.cbslocal.com/tag/someone-like-you/",
"text":"Someone Like You « 101.9 AMP Radio\n\nArizona\nCalifornia\nConnecticut\nFlorida\nGeorgia\nIllinois\nMaryland\nMassachusetts\nMichigan\nMinnesota\nMissouri\nNevada\n\nOn-Air\n#NowPlaying\nEvents\nContests\nPhotos\nVideo\n#myAMPrewards\n#Trending\n\n101.9 AMP Radio High School Takeover Click Here To Register Your School NOW!!!\n\nTrending: Is YouTube Banning Adele's Music?If you currently listen to your favorite artists music on YouTube, you may soon find yourself sitting in silence at your desk.\n\nHot Right Now: Adele's New Album, Katy Perry And Beyonce Battle On The Charts, Demi Lovato Tweets Support For Selena GomezUnless you've been living under a rock for the past 4 years, you know that Adele is the Queen of breakup albums and we are all her musical servants. She literally has a song for every emotion you go through during a breakup.\n\nAdele Is The Reason The U.K Sold It's One Billionth Music Single DownloadThe accolades just don't stop for Adele, even 2 years after the release of her massive hit album, 21.\n\nAdele's ...",
"annotations":{
 "Paragraph":[{"id":1,"anno":{"start":0,"end":34}},{"id":10,"anno":{"start":36,"end":149}},...],
 "Sentence":[{"id":0,"anno":{"start":0,"end":34}},{"id":9,"anno":{"start":36,"end":149}},...],
 "Token":[{"id":2,"anno":{"start":0,"end":7}},{"id":3,"anno":{"start":8,"end":12}},{"id":4,"anno":{"start":13,"end":16}},...],
 "EntityWithId":[{"id":11,"anno":{"start":36,"end":43,"type":"dbo:Region","id":"dbr:Arizona"}},
                 ...,{"id":70,"anno":{"start":333,"end":338,"type":"dbo:Agent","id":"dbr:Adele"}},...]}}
```

 
## TSV Format

After context set construction, the dataset is a tsv file of contexts between two nodes, grouped by node-pair.
The format is "simple tsv" meaning that the field separator is always '\t' and the record seperator is always '\n'. No other characters have any special meaning; there are no escapes. If the sentence contained newlines or tabs, they were converted to spaces. This makes it easy to use many unix tools, like cut, awk and sort. The final files will be grouped by ID 1 and ID 2, meaning all of the contexts for the same node pair will be continuous. 

The ID fields give the DBpedia ID for the two nodes.  The Type fields give the types from the EDL system. The Char Span fields give character offsets for the two nodes in the Context. The Relations field gives the ground truth relations between the two nodes separated by commas, note that this will be duplicated for every context of an ID pair. The relations field is empty for two unrelated nodes. The Context is the text snippet where the two nodes co-occur. Finally, the URL functions as the ID of the source document for the Context.

### Example

ID 1 | ID 2 | Type 1 | Type 2 | Char Span 1 | Char Span 2 | Relations | Context | URL  
---|---|---|---|---|---|---|---|---
dbr: The_Beatles | dbr: Tony_Sheridan | dbo: Group | dbo: Artist | [150,161) | [172,185) | <dbo:associatedBand,<odp:isMemberOf,<dbo:associatedMusicalArtist,<odp:hasMember | Ain't She Sweet (album) - Wikipedia, the free encyclopedia Ain't She Sweet was an American album featuring four tracks recorded in Hamburg in 1961 by **The Beatles** featuring **Tony Sheridan** (except for the title song with vocal ... Ain't She Sweet by Susan Elizabeth Phillips  Reviews ... Ain't She Sweet has 13,958 ratings and 716 reviews. | http://2snowflakes. blogspot.com/2015/05/
dbr: Dion_Waiters | dbr: Shooting_guard | dbo: Athlete | unk | [65,77) | [50,64) | >dbo:position | During the season Irving was accused by Cavaliers **shooting guard** **Dion Waiters** of playing buddy ball with power forward Tristan Thompson which led to a rift between them. | http:// 300lbsofsportsknowledge .com/tag/dan-gilbert/
dbr: Dion_Waiters | dbr: Shooting_guard | dbo: Athlete | unk | [88,100) | [73,87) | >dbo:position | The Cavaliers also have youngsters in power forward Tristan Thompson and **shooting guard** **Dion Waiters** who will look to be on board as members of the new James Gang in Cleveland. | http:// 300lbsofsportsknowledge .com/tag/stan-van-gundy/


To see examples of context sets for node pairs that are related use:
```
awk  -F $'\t' '$7!=""' dataset/contextSets/contexts-part0.tsv | head
```