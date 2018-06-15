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
package com.ibm.research.ai.ki.kbp.unary;

import java.io.*;
import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.kbp.RelexConfig.TypeStyle;
import com.ibm.research.ai.ki.util.*;

import com.google.common.collect.*;

public class UnaryRelexTensors implements IRelexTensors<UnaryRelexMention> {
	private static final long serialVersionUID = 1L;
	
	RelexConfig config;
	Map<String,Integer> wordNdx;
	Map<String,Integer> typeNdx;
	Map<String,Integer> relationNdx;
	
	public UnaryRelexTensors(RelexConfig config) {
		this.config = config;
		this.wordNdx = RelexVocab.getWordNdx(
		        config.word2vecModelFile != null ? 
		            new File(config.word2vecModelFile) :
		            new File(config.convertDir, RelexDatasetFiles.wordVectors));
		this.typeNdx = HashMapUtil.arrayToIndex(config.entityTypes);
		this.relationNdx = HashMapUtil.arrayToIndex(config.relationTypes);
	}
	
	@Override
	public String[] getTypes() {
	    return config.entityTypes;
	}
	
	@Override
	public String[] getRelations() {
		return HashMapUtil.indexToArray(relationNdx, String.class);
	}
	
	/**
	 * So this should operate on a PairRDD<String,List<RelexMention>>. Converting it to a RDD<byte[]>, 
	 * with TensorFileWriter. Then we save to RDD<String> using Base64 encode. 
	 * 
	 * In apply mode, we will flatMap this on a stream of Collection<RelexMention>.
	 * @param tokenizer
	 * @param mentionSet
	 * @return
	 */
	public List<Object[]> makeInstances(Annotator tokenizer, Collection<UnaryRelexMention> fullMentionSet) {
		if (fullMentionSet.size() < config.minMentionSet)
			return Collections.EMPTY_LIST;
		
		UnaryRelexMention mention1 = fullMentionSet.iterator().next();
		List<Object[]> instances = new ArrayList<>();
		int numPartitions = (int)Math.ceil(fullMentionSet.size()/(double)config.maxMentionSet);
		for (List<UnaryRelexMention> mentionSet : CollectionUtil.partition(fullMentionSet, numPartitions)) {
			List<List<String>> words = new ArrayList<>();
			Span[] entSpans = new Span[mentionSet.size()];
			int[] sentStarts = new int[mentionSet.size()];
			int[][] poolPieces = new int[mentionSet.size()][];
			
			int sentNdx = 0;
			int sumWords = 0;
			for (UnaryRelexMention mention : mentionSet) {
				sentStarts[sentNdx] = sumWords;
				Document doc = new Document(mention.sentence);
				tokenizer.process(doc);
				List<Token> tokens = doc.getAnnotations(Token.class);
				words.add(Lists.transform(tokens, t -> RelexVocab.normalized(t.coveredText(doc))));
				sumWords += tokens.size();
				
				entSpans[sentNdx] = Span.toSegmentSpan(mention.span, tokens);
				
				Span argSpan = entSpans[sentNdx];
				int[] poolPiece = new int[] {Math.max(1,argSpan.start), Math.min(tokens.size()-1, argSpan.end)};
				poolPieces[sentNdx] = poolPiece;
				
				++sentNdx;
			}
			
			int[][] wordIndicesMatrix = new int[2][sumWords];
			for (int senti = 0; senti < words.size(); ++senti) {
				List<String> swords = words.get(senti);
				Span entSpan = entSpans[senti];
				int sentStart = sentStarts[senti];
				for (int wi = 0; wi < swords.size(); ++wi) {
					//NOTE: we do -1 for OOV in sgd.dl but TensorFlow will need a special OOV token
					wordIndicesMatrix[0][sentStart+wi] = Lang.NVL(wordNdx.get(swords.get(wi)), -1);
					wordIndicesMatrix[1][sentStart+wi] = RelexTensors.toPositionEmbeddingNdx(wi, entSpan, config.maxPositionEmbeddings);
				}
			}
			
			int[] gtRels = new int[relationNdx.size()];
			for (String t : mention1.relations) {
			    Integer rndx = relationNdx.get(t);
			    if (rndx != null) //note that we may pass a relationNdx that is a subset of what the IRelexMentions were built with.
			        gtRels[rndx] = 1;
			}
			
			instances.add(new Object[] {
					mention1.id, //to track which entities we are predicting relations for
					wordIndicesMatrix, poolPieces, sentStarts, getTypeTensor(mentionSet), gtRels});
		}
		return instances;
	}
	
	private Object getTypeTensor(List<UnaryRelexMention> mentionSet) {
		if (config.typeStyle == TypeStyle.none) {
			return 0;
		} else if (config.typeStyle == TypeStyle.single) {
			UnaryRelexMention mention1 = mentionSet.get(0);
			return Lang.NVL(typeNdx.get(mention1.type), -1);
		} else if (config.typeStyle == TypeStyle.avgOverMentions) {
			int[] argTypes = new int[mentionSet.size()];
			for (int i = 0; i < mentionSet.size(); ++i) {
				argTypes[i] = Lang.NVL(typeNdx.get(mentionSet.get(i).type), -1);
			}
			return argTypes;
		} else {
			throw new UnsupportedOperationException(config.typeStyle.toString());
		}
	}

}
