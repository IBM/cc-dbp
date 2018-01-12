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

import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.kbp.RelexConfig.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.io.*;

public class RelexTensorDataset<M extends IRelexMention> extends SimpleSparkJob {
	private static final long serialVersionUID = 1L;
	
	protected final RelexConfig config;
	protected final IRelexTensors<M> rt;
	protected final Class<M> mentionClass;
	protected final Object2IntOpenHashMap<String> gsplits;
	
	public RelexTensorDataset(RelexConfig config, IRelexDatasetManager<M> rdmanager, Object2IntOpenHashMap<String> gsplits) {
	    this.config = config;
		this.rt = rdmanager.getTensorMaker();
		this.mentionClass = rdmanager.getMentionClass();
		this.gsplits = gsplits;
		
	}
	
	public JavaRDD<String> buildInstances(JavaPairRDD<String,List<M>> mentionSets) {
		JavaRDD<Object[]> instances = mentionSets.flatMap(new FlatMapFunction<Tuple2<String,List<M>>, Object[]>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Object[]> call(Tuple2<String, List<M>> t) throws Exception {
			    try {
				return rt.makeInstances(Tokenizer.getTokenizer(config), t._2).iterator();
			    } catch (NullPointerException npe) {
			        npe.printStackTrace();
			        System.err.println("In RelexTensorDataset.buildInstances makeInstances");
			        throw npe;
			    }
			}
			
		});
		
		int skippedMentions = RelexTensors.getSkippedMentionCount();
		if (skippedMentions > 0) {
		    System.err.println("WARNING: "+skippedMentions+" mentions were skipped, likely because entities didn't match tokens!");
		}
		
		return instances.map(new Function<Object[],String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Object[] v1) throws Exception {
				return Base64.getEncoder().encodeToString(TensorFileWriter.byteArrayTensorSet(v1));
			}
		});
	}
	
	@Override
	public JavaRDD<String> process(JavaRDD<String> tsvLines) throws Exception {
	    //TODO: should pass a filter constructed according to a type-pair filter; and set the relation2id.tsv file to the desired subset of relations
		JavaPairRDD<String,List<M>> mentionSets = RelexTsvDataset.getMentionSets(
		        tsvLines, mentionClass, gsplits, config.documentFractionForTensorDataset, null);
		return buildInstances(mentionSets);
	}

	
    /**
     * Example args 
	-in /blekko/data/relexMentions.tsv -out /blekko/data/tensors50i.bin.b64 -config /disks/diskm/mglass/blekko/relexConfig.properties
     * @param args
     */
    @SuppressWarnings("unchecked")
	public static void main(String[] args) {
    	Options options = new Options();
    	options.addOption("config", true, "");
		options.addOption("in", true, "");
		options.addOption("out", true, "");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);  
		} catch (ParseException pe) {
			Lang.error(pe);
		}
	
		String inputPath = cmd.getOptionValue("in");
		String outputPath = cmd.getOptionValue("out");
		
		String configProperties = cmd.getOptionValue("config");
		RelexConfig config = new RelexConfig();
		config.fromString(FileUtil.readFileAsString(configProperties));
		
		RelexTensorDataset rtd = new RelexTensorDataset(config, config.getManager(), null);
		rtd.run("RelexTensorDataset", inputPath, outputPath);
		
		//get the dataset splits from the RelexConfig
		//into multiple directories, splitting train/validate/test
		Pair<File,Double>[] splits = new Pair[config.datasetSpitFractions.length];
		for (int si = 0; si < splits.length; ++si) {
			splits[si] = Pair.of(
					new File(config.convertDir, config.datasetSplitNames[si]+RelexDatasetFiles.dataDirSuffix), 
					config.datasetSpitFractions[si]);
		}
		Base64ToBinary.convert(config, rtd.fs, outputPath, splits);
	}	
}
