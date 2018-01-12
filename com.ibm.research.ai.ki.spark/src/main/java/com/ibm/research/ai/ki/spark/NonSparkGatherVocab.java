/**
 * cc-dbp-dataset
 *
 * Copyright (c) 2017 IBM
 *
 * The author licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.research.ai.ki.spark;

import java.io.*;
import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.FileUtil;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

/**
 * This is a straightforward vocab gathering program. It reads from hdfs but is not Spark.
 * Created to compare the performance of the Spark GatherRelexVocab (and maybe later GatherRelexStats) to
 * a straightforward k parallel tasks + final merge.
 * I think even in a single thread it is faster than Spark.
 * @author mrglass
 *
 */
public class NonSparkGatherVocab {
    /**
java -cp com.ibm.sai.ie.spark-1.0.1-SNAPSHOT-jar-with-dependencies.jar com.ibm.sai.ie.spark.NonSparkGatherVocab \
hdfs://kg8.pok.ibm.com:8020 /blekko/data/relexDirAlchemyNER/relexMentions.tsv /disks/diskm/mglass/dbpAlchemyNER/origRelexConfig0.properties 0 10
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String hdfsBase = args[0];
        String tsvDir = args[1];
        String relexConfigFile = args[2];
        int processNumber = Integer.parseInt(args[3]);
        int totalProcesses = Integer.parseInt(args[4]);
        

        RelexConfig config = new RelexConfig();
        config.fromString(FileUtil.readFileAsString(relexConfigFile));
        
        Class<? extends IRelexMention> c = config.getManager().getMentionClass();
        
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            if (!hdfsBase.endsWith("/"))
                hdfsBase = hdfsBase + "/";
            conf.set("fs.defaultFS", hdfsBase);
            conf.set("fs.hdfs.impl", 
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
                );
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
                );
            fs = FileSystem.get(conf);
            System.out.println("Using FileSystem at "+hdfsBase); 
        } catch (Exception e) {
            Lang.error(e);
        }
        
        RelexVocab vocab = new RelexVocab();
        //NOTE: stats requires the mentions be grouped. Unless you want to maintain a set of already processed ids
        //RelexStats stats = new RelexStats(FileUtil.loadObjectFromFile(config.groundTruthFile));
        Annotator tokenizer = new Pipeline(new ClearNLPTokenize());
        tokenizer.initialize(new Properties());
        PeriodicChecker report = new PeriodicChecker(100);
        RemoteIterator<LocatedFileStatus> rit = fs.listFiles(new Path(tsvDir), true);
        while(rit.hasNext()) {
            
            Path p = rit.next().getPath();
            if (new Random(p.getName().hashCode()).nextInt(totalProcesses) != processNumber)
                continue;
            if (report.isTime())
                System.out.println("On file "+report.checkCount()+" after "+Lang.milliStr(report.elapsedTime()));
            try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(p)))) {
                String line = null;
                while ((line = r.readLine()) != null) {
                    IRelexMention rm = c.newInstance();
                    rm.fromString(line);
                    
                    vocab.add(rm, tokenizer);
                    //stats.
                    //periodic vocab trim
                    if (vocab.vocabSize() > (int)(config.vocabLimit*1.5))
                        config.vocabMinCount = vocab.limitVocab(config.vocabMinCount, (int)(config.vocabLimit*1.5));
                }
            }
        }
        vocab.limitVocab(config.vocabMinCount, config.vocabLimit);
        
        FileUtil.saveObjectAsFile(new File(config.convertDir, "vocab_"+(processNumber+1)+"_of_"+totalProcesses+".ser.gz"), vocab);
        
        System.out.println("Took "+Lang.milliStr(report.elapsedTime())+" final vocabMinCount = "+config.vocabMinCount+", vocab size is "+vocab.vocabSize());
    }
}
