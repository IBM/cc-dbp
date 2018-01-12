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

import org.apache.commons.cli.*;
import org.apache.log4j.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.kbp.unary.*;
import com.ibm.research.ai.ki.util.*;

import scala.Tuple2;

public class GatherRelexStats implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Get statistics on the dataset
	 * @param gt may be null, in this case ground truth coverage is not measured
	 * @param mentionSets the RDD of the RelexMentions, grouped by key
	 * @return
	 */
	public static <MC extends IRelexMention> RelexStats gatherStats(IGroundTruth gt, JavaPairRDD<String,List<MC>> mentionSets) {
		return mentionSets.aggregate(
			new RelexStats(gt), 
			new Function2<RelexStats, Tuple2<String,List<MC>>, RelexStats>() {
				private static final long serialVersionUID = 1L;
				@Override
				public RelexStats call(RelexStats v1, Tuple2<String, List<MC>> v2) throws Exception {
					v1.noteMention(v2._2);
					return v1;
				}	
			}, 
			new Function2<RelexStats, RelexStats, RelexStats>() {
				private static final long serialVersionUID = 1L;
				@Override
				public RelexStats call(RelexStats v1, RelexStats v2) throws Exception {
					v1.merge(v2);
					return v1;
				}			
			});
	}
	
	public static void sparkRun(JavaSparkContext sc, RelexConfig config, IRelexDatasetManager<? extends IRelexMention> rdmanager, String outputHDFSDir) {
	    outputHDFSDir = FileUtil.ensureSlash(outputHDFSDir);
	    JavaRDD<String> tsvLines = sc.textFile(outputHDFSDir+RelexDatasetFiles.hdfsMentions);
        Object2IntOpenHashMap<String> gsplits = FileUtil.loadObjectFromFile(new File(config.convertDir, RelexDatasetFiles.groupSplits));       
        RelexStats relexStats = gatherStats(
                null, //rdmanager.getGroundTruth(), 
                RelexTsvDataset.getMentionSets(
                        tsvLines, rdmanager.getMentionClass(), gsplits, config.documentFractionForTensorDataset, null));
        FileUtil.writeFileAsString(new File(config.convertDir, "relexStatsNoGT.txt"), relexStats.toString());
	}
	
	/**
	 * Example args:
	   -out /blekko/data/permId/relexBinaryEL/ -config /disks/diskm/mglass/permId/relexConfigBinaryPermId.properties
	 * @param args
	 */
	public static void main(String[] args) {
	    Options options = new Options();
        options.addOption("config", true, "A RelexConfig in properties file format");
        options.addOption("out", true, "The HDFS output directory");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);  
        } catch (ParseException pe) {
            System.err.println(Lang.stringList(args, ";; "));
            Lang.error(pe);
        }
        String outputHDFSDir = FileUtil.ensureSlash(cmd.getOptionValue("out"));
        
        String configProperties = cmd.getOptionValue("config");
        RelexConfig config = new RelexConfig();
        config.fromString(FileUtil.readFileAsString(configProperties));
        
        IRelexDatasetManager<? extends IRelexMention> rdmanager = config.getManager();
        
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
        try (JavaSparkContext sc = new JavaSparkContext(conf.setAppName("Relex Gather Stats ("+
                rdmanager.getMentionClass().getSimpleName()+")"))) 
        {
            sc.setLogLevel("ERROR");
            Logger.getRootLogger().setLevel(Level.ERROR);
            sparkRun(sc, config, rdmanager, outputHDFSDir);
        }
	}
}
