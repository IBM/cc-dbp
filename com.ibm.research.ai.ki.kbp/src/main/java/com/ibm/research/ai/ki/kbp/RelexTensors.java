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
package com.ibm.research.ai.ki.kbp;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.commons.cli.*;

import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.RelexConfig.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.io.*;

/**
 * Converts the TSV dataset to one with tensors for use in a CNN deep learning relation prediction system.
 * @author mrglass
 *
 */
public class RelexTensors implements IRelexTensors<RelexMention> {
	private static final long serialVersionUID = 1L;

	//Load all these in the Spark master
	
	protected Map<String,Integer> wordNdx;
	
	protected Map<String,Integer> relationNdx;
	
	//only one of these will be set
	protected Map<String,Integer> typeNdx;
	protected Map<String,float[]> id2TypeVector;
	
	protected RelexConfig config;
	
	public RelexTensors(RelexConfig config) { 
		this.config = config;
		File w2vModel = config.word2vecModelFile != null ? 
                new File(config.word2vecModelFile) :
                new File(config.convertDir, RelexDatasetFiles.wordVectors);
        if (!w2vModel.exists())
            throw new IllegalArgumentException("No such file: "+w2vModel.getAbsolutePath());
		this.wordNdx = RelexVocab.getWordNdx(w2vModel);
		this.relationNdx = HashMapUtil.arrayToIndex(config.relationTypes);
		if (config.entityTypeVectorFile != null)
			setTypeVectors(RelexVocab.readTypeVectors(new File(config.entityTypeVectorFile)));
		else
			setTypeNdx(TypeStyle.single, HashMapUtil.arrayToIndex(config.entityTypes));	
	}
	
	public String[] getTypes() {
		if (typeNdx == null)
			return null;
		return HashMapUtil.indexToArray(typeNdx, String.class);
	}
	
	public String[] getRelations() {
		return HashMapUtil.indexToArray(relationNdx, String.class);
	}
	
	/**
	 * One of setTypeNdx or setTypeVectors
	 * @param typeStyle
	 * @param typeNdx
	 */
	protected void setTypeNdx(TypeStyle typeStyle, Map<String,Integer> typeNdx) {
		if (typeStyle == TypeStyle.vector)
			throw new IllegalArgumentException();
		this.typeNdx = typeNdx;
		config.typeStyle = typeStyle;
	}
	
	protected void setTypeVectors(Map<String,float[]> id2TypeVector) {
		config.typeStyle = TypeStyle.vector;
		this.id2TypeVector = id2TypeVector;
	}
	
	private static AtomicInteger skippedMentions = new AtomicInteger(0);
	
	public static int getSkippedMentionCount() {
	    return skippedMentions.get();
	}
	
	/**
	 * In Spark this should operate on a PairRDD<String,List<RelexMention>>. Converting it to a RDD<byte[]>, 
	 * with TensorFileWriter. Then we save to RDD<String> using Base64 encode. 
	 * 
	 * In apply mode, we will flatMap this on a stream of Collection<RelexMention>.
	 * @param tokenizer
	 * @param mentionSet
	 * @return
	 */
	public List<Object[]> makeInstances(Annotator tokenizer, Collection<RelexMention> fullMentionSet) {
		if (fullMentionSet.size() < config.minMentionSet)
			return Collections.EMPTY_LIST;
		
		RelexMention mention1 = fullMentionSet.iterator().next();
		List<Object[]> instances = new ArrayList<>();
		int numPartitions = (int)Math.ceil(fullMentionSet.size()/(double)config.maxMentionSet);
		for (List<RelexMention> mentionSet : CollectionUtil.partition(fullMentionSet, numPartitions)) {
		    
			List<List<String>> words = new ArrayList<>();
			Span[] arg1Spans = new Span[mentionSet.size()];
			Span[] arg2Spans = new Span[mentionSet.size()];
			int[] sentStarts = new int[mentionSet.size()];
			int[][] poolPieces = new int[mentionSet.size()][];
			
			int sentNdx = 0;
			int sumWords = 0;
			for (Iterator<RelexMention> mit = mentionSet.iterator(); mit.hasNext(); ) {
			    RelexMention mention = mit.next();
				Document doc = new Document(mention.sentence);
				tokenizer.process(doc);
				List<Token> tokens = doc.getAnnotations(Token.class);
				
				arg1Spans[sentNdx] = Span.toSegmentSpan(mention.span1, tokens);
				arg2Spans[sentNdx] = Span.toSegmentSpan(mention.span2, tokens);
				//skip this mention if we can't get token spans for the entities
				if (arg1Spans[sentNdx] == null || arg2Spans[sentNdx] == null) {
				    skippedMentions.addAndGet(1);
				    //Note that this shouldn't happen if we use detection of EntityWithId that is consistent with the tokenization
				    mit.remove();
				    continue;
				}
				
				words.add(Lists.transform(tokens, t -> RelexVocab.normalized(t.coveredText(doc))));
                sentStarts[sentNdx] = sumWords;
                sumWords += tokens.size();
				
				Span argSpan = Span.coveringSpan(arg1Spans[sentNdx],arg2Spans[sentNdx]);
				int[] poolPiece = new int[] {Math.max(1,argSpan.start), Math.min(tokens.size()-1, argSpan.end)};
				poolPieces[sentNdx] = poolPiece;
				
				++sentNdx;
			}
			if (sentNdx < sentStarts.length) {
			    arg1Spans = Arrays.copyOf(arg1Spans, sentNdx);
			    arg2Spans = Arrays.copyOf(arg2Spans, sentNdx);
			    sentStarts = Arrays.copyOf(sentStarts, sentNdx);
			    poolPieces = Arrays.copyOf(poolPieces, sentNdx);
			}
			
			int[][] wordIndicesMatrix = new int[3][sumWords];
			for (int senti = 0; senti < words.size(); ++senti) {
				List<String> swords = words.get(senti);
				Span arg1Span = arg1Spans[senti];
				Span arg2Span = arg2Spans[senti];
				int sentStart = sentStarts[senti];
				for (int wi = 0; wi < swords.size(); ++wi) {
					//NOTE: we do -1 for OOV in sgd.dl but TensorFlow will need a special OOV token
					Integer wndx = wordNdx.get(swords.get(wi));
					//if it is a digit sequence, generalize to digit sequences of that length
					if (wndx == null && Lang.isInteger(swords.get(wi))) {
						wndx = wordNdx.get(RelexVocab.toDigitSequence(swords.get(wi)));
					}
					wordIndicesMatrix[0][sentStart+wi] = Lang.NVL(wndx, -1);
					wordIndicesMatrix[1][sentStart+wi] = toPositionEmbeddingNdx(wi, arg1Span, config.maxPositionEmbeddings);
					wordIndicesMatrix[2][sentStart+wi] = toPositionEmbeddingNdx(wi, arg2Span, config.maxPositionEmbeddings);
				}
			}
			
			int[] gtRels = new int[relationNdx.size()];
			for (String r : mention1.relTypes) {
			    Integer rndx = relationNdx.get(r);
			    if (rndx != null) //note that we may pass a relationNdx that is a subset of what the IRelexMentions were built with.
			        gtRels[rndx] = 1;
			}
			
			instances.add(new Object[] {
					mention1.groupId(), //to track which entities we are predicting relations for
					wordIndicesMatrix, poolPieces, sentStarts, getTypeTensor(mentionSet), gtRels});
		}
		return instances;
	}
	
	//argTypes should either be a two row float matrix with #cols == numTypes 
	//or a 2 element int vector
	//or a 2 row int matrix with #cols == num mentions
	private Object getTypeTensor(List<RelexMention> mentionSet) {
		if (config.typeStyle == TypeStyle.none) {
			return new int[] {0,0};
		} else if (config.typeStyle == TypeStyle.single) {
			RelexMention mention1 = mentionSet.get(0);
			return new int[] {
					Lang.NVL(typeNdx.get(mention1.type1), -1), 
					Lang.NVL(typeNdx.get(mention1.type2), -1)};
		} else if (config.typeStyle == TypeStyle.avgOverMentions) {
			int[][] argTypes = new int[2][mentionSet.size()];
			for (int i = 0; i < mentionSet.size(); ++i) {
				RelexMention m = mentionSet.get(i);
				argTypes[0][i] = Lang.NVL(typeNdx.get(m.type1), -1);
				argTypes[1][i] = Lang.NVL(typeNdx.get(m.type2), -1);
			}
			return argTypes;
		} else if (config.typeStyle == TypeStyle.vector) {
			//permit a type vector (for STR Diebold)
			RelexMention m1 = mentionSet.get(0);
			return new float[][] {id2TypeVector.get(m1.id1), id2TypeVector.get(m1.id2)};
		} else {
			throw new UnsupportedOperationException(config.typeStyle.toString());
		}
	}
	
	public static int toPositionEmbeddingNdx(int pos, Span arg, int maxPositionEmbeddings) {
		int posDiff = 0;
		if (pos < arg.start)
			posDiff = pos - arg.start;
		else if (pos > arg.end-1)
            posDiff = (arg.end-1) - pos;
		int ndx = posDiff + maxPositionEmbeddings/2;
		if (ndx >= maxPositionEmbeddings)
			return maxPositionEmbeddings - 1;
		if (ndx < 0)
			return 0;
		return ndx;
	}
	
	/**
	 * Main method for non-Spark case
	 * 
	 * Remaining args are the tsv files to convert.
	 * 
	 Example Args:

	 -convertDir /baseDir/dataset 
	 -config /baseDir/dataset/relexConfig.properties
	 
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Options options = new Options();
		options.addOption("convertDir", true, "");
		options.addOption("config", true, "");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);  
		} catch (ParseException pe) {
			Lang.error(pe);
		}
		
		String convertDir = cmd.getOptionValue("convertDir");
		String configProperties = cmd.getOptionValue("config", new File(convertDir, "relexConfig.properties").getAbsolutePath());
		RelexConfig config = new RelexConfig();
		config.fromString(FileUtil.readFileAsString(configProperties));
		if (convertDir != null)
			config.convertDir = convertDir;
		
		RelexTensors rt = new RelexTensors(config);
		
		Annotator tokenizer = Tokenizer.getTokenizer(config);
		
		PeriodicChecker report = new PeriodicChecker(100);
		for (String tsvFiles : cmd.getArgs()) {
			File tsv = new File(tsvFiles);
			try (TensorFileWriter writer = new TensorFileWriter(new File(convertDir, tsv.getName()))) {
				for (File tsvi : new FileUtil.FileIterable(tsv)) {
					for (List<RelexMention> ms : RelexMentionReader.getSetReader(tsvi, RelexMention.class)) {
						if (report.isTime()) {
							System.out.println("On mention set "+report.checkCount());
						}
						for (Object[] ts : rt.makeInstances(tokenizer, ms)) {
							writer.addTensorSet(ts);
						}
					}
				}
			}
		}
		
	}


}
