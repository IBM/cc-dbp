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

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.kbp.unary.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.FileUtil;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.storage.*;

public class RelexBuildDataset implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final RelexConfig config;
    protected final IRelexDatasetManager<? extends IRelexMention> rdmanager;

    public RelexBuildDataset(RelexConfig config) {
        this.config = config;
        this.rdmanager = config.getManager();
    }
    
    static void saveRelexConfig(String prefix, RelexConfig config) {
        for (int fi = 0; fi < 50; ++fi) {
            File configFile = new File(config.convertDir, prefix+"RelexConfig"+fi+".properties");
            if (!configFile.exists()) {
                FileUtil.writeFileAsString(configFile, config.toString());
                System.out.println("Wrote configuration to "+configFile.getAbsolutePath());
                break;
            }
        }
    }

    public static boolean statsWithGT = false;
    
    public void sparkRun(JavaSparkContext sc, FileSystem fs, String inputCorpus, String outputHDFSDir) throws Exception {
        outputHDFSDir = FileUtil.ensureSlash(outputHDFSDir);
        System.out.println(config.toString());
        saveRelexConfig("orig", config);
        //make the output directory if it does not exist
        if (!fs.exists(new Path(outputHDFSDir)))
            fs.mkdirs(new Path(outputHDFSDir));

        //NOTE: we use file existing as indication of using the already built file
        //  so we can resume from partial completion
        
        //CONSIDER: do the split here instead, just give it the range
        JavaRDD<String> tsvLines = null;
        Object2IntOpenHashMap<String> gsplits = null;
        long startTime = System.currentTimeMillis();
        if (!fs.exists(new Path(outputHDFSDir+RelexDatasetFiles.hdfsMentions))) {
            JavaRDD<String> input = sc.textFile(inputCorpus);
            RelexTsvDataset relexTsv = new RelexTsvDataset(rdmanager.getGroundTruth(), config, rdmanager.getTsvMaker());
            tsvLines = relexTsv.process(input);
            tsvLines.persist(StorageLevel.MEMORY_AND_DISK());
            gsplits = relexTsv.gsplits;
            tsvLines.saveAsTextFile(outputHDFSDir+RelexDatasetFiles.hdfsMentions);
            System.out.println("Saved "+outputHDFSDir+RelexDatasetFiles.hdfsMentions+" after "+Lang.milliStr(System.currentTimeMillis()-startTime));
            saveRelexConfig("postTsv", config); //negative sampling stuff updated here
            FileUtil.saveObjectAsFile(new File(config.convertDir, RelexDatasetFiles.groupSplits), gsplits);
        } else {
            tsvLines = sc.textFile(outputHDFSDir+RelexDatasetFiles.hdfsMentions);
            gsplits = FileUtil.loadObjectFromFile(new File(config.convertDir, RelexDatasetFiles.groupSplits));
            System.out.println("Loaded existing "+outputHDFSDir+RelexDatasetFiles.hdfsMentions+" after "+Lang.milliStr(System.currentTimeMillis()-startTime));
        }
        
        //CONSIDER: very possible that these later stages are better done outside spark
        
        //Gatherstats
        //get dataset stats, and write out
        if (config.gatherRelexStats && !new File(config.convertDir, "relexStats.txt").exists()) {
            RelexStats relexStats = GatherRelexStats.gatherStats(null, 
                    RelexTsvDataset.getMentionSets(tsvLines, rdmanager.getMentionClass(), gsplits, 1.0, null));
            FileUtil.writeFileAsString(new File(config.convertDir, "relexStats.txt"), relexStats.toString());
        }
        
        //GatherRelexVocab
        if (!new File(config.convertDir, RelexDatasetFiles.wordVectors).exists()) 
        {
            startTime = System.currentTimeMillis();
            GatherRelexVocab v = new GatherRelexVocab(config, rdmanager.getMentionClass());
            v.process(tsvLines);
            v = null;
            System.out.println("Saved vocabulary after "+Lang.milliStr(System.currentTimeMillis()-startTime));
            saveRelexConfig("postVocab", config); //vocab limits updated here
        } else {
            System.out.println("Using existing vocabulary at "+
                    new File(config.convertDir, RelexDatasetFiles.wordVectors).getAbsolutePath());
        }
        
        //RelexTensors
        if (!fs.exists(new Path(outputHDFSDir+hdfsTensorsFile()))) {
            startTime = System.currentTimeMillis();
            RelexTensorDataset<? extends IRelexMention> rtd = new RelexTensorDataset(config, rdmanager, gsplits);
            JavaRDD<String> b64tensors = rtd.process(tsvLines);
            rtd = null;
            b64tensors.saveAsTextFile(outputHDFSDir+hdfsTensorsFile());
            System.out.println("Built relex tensor dataset (Base64 in HDFS) after "+Lang.milliStr(System.currentTimeMillis()-startTime));
        } else {
            System.out.println("Using existing relex tensor dataset (Base64 in HDFS)");
        }
    }
    
    public void postSparkRun(FileSystem fs, String outputHDFSDir, boolean hdfsOnly) {
        outputHDFSDir = FileUtil.ensureSlash(outputHDFSDir);
        //we will do only hdfs when gathering all of the data to run apply
        if (!hdfsOnly) {        
            //get the dataset splits from the RelexConfig
            //into multiple directories, splitting train/validate/test
            Pair<File,Double>[] splits = new Pair[config.datasetSpitFractions.length];
            System.out.println("Tensor dataset splits are:");
            for (int si = 0; si < splits.length; ++si) {
                splits[si] = Pair.of(
                        new File(config.convertDir, config.datasetSplitNames[si]+RelexDatasetFiles.dataDirSuffix), 
                        config.datasetSpitFractions[si]);
                System.out.println("   "+config.datasetSplitNames[si]+": "+config.datasetSpitFractions[si]);
            }
            
            Base64ToBinary.convert(config, fs, outputHDFSDir+hdfsTensorsFile(), splits);
        }
        saveRelexConfig("final", config); //final save, but should be no different than after GatherRelexVocab
    }
    
    //name the tensors according to the document fraction they are based on
    protected String hdfsTensorsFile() {
        if (config.documentFractionForTensorDataset > 0.0 && config.documentFractionForTensorDataset < 1.0)
            return "tensors_" + ((int)(config.documentFractionForTensorDataset*1000)) + ".b64";
        return RelexDatasetFiles.hdfsTensors;
    }
    
    /**
     * 
     * @param inputCorpus
     * @param outputHDFSDir
     * @param configProperties
     * @param hdfsOnly
     */
    public void run(String inputCorpus, String outputHDFSDir, boolean hdfsOnly) throws Exception {
        
        FileSystem fs = null;
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //conf.set("spark.kryo.registrationRequired", "true");
        conf.registerKryoClasses(new Class[] {
                RelexMention.class, Span.class, 
                RelexTsvDataset.CountAndPositive.class,
                RelexVocab.class, Object2IntOpenHashMap.class, Int2IntOpenHashMap.class, ObjectSet.class, RelexStats.class,
                GroundTruth.class, UnaryGroundTruth.class, RelexConfig.class,
                MutableDouble.class, Pair.class,
                HashSet.class, HashMap.class, ArrayList.class});
        try (JavaSparkContext sc = new JavaSparkContext(conf.setAppName("Relex Dataset Construction ("+
                rdmanager.getMentionClass().getSimpleName()+")"))) 
        {
            sc.setLogLevel("ERROR");
            Logger.getRootLogger().setLevel(Level.ERROR);
            
            fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration());       
            sparkRun(sc, fs, inputCorpus, outputHDFSDir);
        } catch (Exception e) {
        	e.printStackTrace();
        	System.err.println("FATAL ERROR"); System.err.flush();
        	Thread.sleep(10000);
            Lang.error(e);
        }
        System.out.println("Finished Spark phase"); System.out.flush();
        
        postSparkRun(fs, outputHDFSDir, hdfsOnly);
    }
    
    /**
     * Example args:
       -config /disks/diskm/mglass/iswc/permid/relexConfigISWC2.properties -in /iswc2017/trainCorpus/ -out /iswc2017/rkiDataset2
       
       -config /disks/diskm/mglass/permId/relexConfigBinaryPermId.properties -in /blekko/data/financialDocsAllLinked.ser.gz.b64 -out /blekko/data/permId/relexBinaryEL
       
       -config /disks/diskm/mglass/permId/relexConfigUnaryPermId.properties -in /blekko/data/financialDocsAllLinked.ser.gz.b64 -out /blekko/data/permId/relexUnaryEL
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("config", true, "A RelexConfig in properties file format");   
        options.addOption("in", true, "The input corpus, a document collection with EntityWithId, Token, and Sentence annotations");
        options.addOption("out", true, "The HDFS output directory");
        options.addOption("hdfsOnly", false, "Don't pull tensor dataset out of hdfs");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);  
        } catch (ParseException pe) {
            System.err.println(Lang.stringList(args, ";; "));
            Lang.error(pe);
        }
        
        String configProperties = cmd.getOptionValue("config");
        if (configProperties == null)
        	throw new IllegalArgumentException("Must supply config.");
        String inputCorpus = cmd.getOptionValue("in");
        String outputHDFSDir = FileUtil.ensureSlash(cmd.getOptionValue("out"));
        boolean hdfsOnly = cmd.hasOption("hdfsOnly");
        
        RelexConfig config = new RelexConfig();
        config.fromString(FileUtil.readFileAsString(configProperties));
        
        RelexBuildDataset ra = new RelexBuildDataset(config);
        ra.run(inputCorpus, outputHDFSDir, hdfsOnly);
    }
}
