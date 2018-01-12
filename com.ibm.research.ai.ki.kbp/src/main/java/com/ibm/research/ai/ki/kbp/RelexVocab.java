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

import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kbp.embeddings.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;

/**
 * useful to gather the vocab on a RelexMention tsv dataset with or without Spark.
 * @author mrglass
 *
 */
public class RelexVocab implements Serializable {
	private static final long serialVersionUID = 1L;
	
	Object2IntOpenHashMap<String> word2count = new Object2IntOpenHashMap<String>();
	ObjectSet<String> types = new ObjectOpenHashSet<String>();
	ObjectSet<String> relations = new ObjectOpenHashSet<String>();
	
	public RelexVocab() {
		//empty constructor
	}
	
	public RelexVocab(IRelexMention m, Annotator tokenizer) {
		this.add(m, tokenizer);
	}
	
	public static String toDigitSequence(String d) {
		return Lang.LPAD("", '#', d.length());
	}
	
	public void add(IRelexMention m, Annotator tokenizer) {
        for (String t : m.getTypes())
            types.add(t);
        
        for (String r : m.getRelations())
            relations.add(r);
        
        for (String tok : m.getTokens(tokenizer)) {
            word2count.addTo(normalized(tok), 1);
            //if it is a number we also view it as a digit sequence of a certain length
            if (Lang.isInteger(tok)) {
            	word2count.addTo(toDigitSequence(tok), 1);
            }
        }
    }
	
	//use this method to convert from RDD<Indices> -> RDDPair<String,Integer>
	// then reduceByKey
	// then filter while count is too high
	// then we'll need to collect
	public Object2IntMap.FastEntrySet<String> getWordCounts() {
		return word2count.object2IntEntrySet();
	}
	
	//use this method to collect the set of types and relations
	public RelexVocab mergeTypesAndRelations(RelexVocab other) {
		this.types.addAll(other.types);
		this.relations.addAll(other.relations);
		return this;
	}
	
	//for single threaded case
	public void merge(RelexVocab other, int vocabLimit) {
		this.types.addAll(other.types);
		this.relations.addAll(other.relations);
		for (Object2IntMap.Entry<String> e : other.word2count.object2IntEntrySet()) {
			this.word2count.addTo(e.getKey(), e.getIntValue());
		}
		
		//reduce to vocabLimit, increasing minCount until done
		double minCount = 2;
		while (this.word2count.size() > vocabLimit) {
			this.removeBelowMinCount((int)Math.ceil(minCount));
			minCount *= 1.5;
		}
	}
	
	public void removeBelowMinCount(int minCount) {	
		List<String> toRemove = new ArrayList<>();
		for (Object2IntMap.Entry<String> e : this.word2count.object2IntEntrySet()) {
			if (e.getIntValue() < minCount) {
				toRemove.add(e.getKey());
			}
		}
		for (String w : toRemove)
			this.word2count.removeInt(w);
	}
	
	/**
	 * 
	 * @param minCount
	 * @param vocabLimit
	 * @return the minCount required to achieve both minCount and vocabLimit
	 */
	public int limitVocab(int removeBelowCount, int vocabLimit) {
	    double minCount = removeBelowCount / 1.5;
	    int nextRemoveBelow = removeBelowCount;
        while (this.word2count.size() > vocabLimit) {
            minCount *= 1.5;
            nextRemoveBelow = (int)Math.ceil(minCount);
            this.removeBelowMinCount(nextRemoveBelow);
        }
        return nextRemoveBelow;
	}
	
	public int vocabSize() {
	    return this.word2count.size();
	}
	
	public Collection<String> getTypes() {
		return types;
	}
	
	public Collection<String> getRelations() {
		return relations;
	}
	

	//CONSIDER put the normalization after the PairRDD, just before the reducebykey
	public static String normalized(String tstr) {
		return tstr.toLowerCase().replaceAll("\\s+", " ");
	}
	
	//this will still be done in a single process, even on spark.
	//we'll just collect the vocab set
	public static void writeVocab(File initialEmbeddings, File finalEmbeddings, Collection<String> thevocab, String[] specialWords) {
		EmbeddingFormat ef = new EmbeddingFormat(initialEmbeddings);
		
		for (int i = 0; i < ef.items.length; ++i)
			ef.items[i] = normalized(ef.items[i]);
		Map<String,Integer> efwordNdx = ef.getItem2Ndx();
		int vlen = ef.itemSize;
		
		Random rand = new Random();
		String[] words = new String[thevocab.size()+specialWords.length];
		float[][] vectors = new float[words.length][];
		int wndx = 0;
		for (wndx = 0; wndx < specialWords.length; ++wndx) {
			words[wndx] = specialWords[wndx];
			vectors[wndx] = DenseVectors.sphericallyRandomFloatVector(vlen, rand);
		}
		
		//report stuff about the vocab
		int oovWords = 0;
		RandomUtil.Sample<String> oovs = new RandomUtil.Sample<>(20);
		for (String v : thevocab) {
			Integer ndx = efwordNdx.get(v);
			if (ndx == null) {
				++oovWords;
				oovs.maybeSave(v);
			}
			words[wndx] = v;
			vectors[wndx] = ndx != null ? ef.vectors[ndx] : DenseVectors.sphericallyRandomFloatVector(vlen, rand);
			++wndx;
		}
		System.out.println("OOV "+oovWords+" out of "+thevocab.size());
		System.out.println("OOV Sample:\n  "+Lang.stringList(oovs, "\n  "));
		ef = new EmbeddingFormat();
		ef.shapes = new int[1][1];
		ef.itemSize = vectors[0].length;
		ef.shapes[0][0] = ef.itemSize;
		ef.hyperparameters = new Properties();
		ef.name = ef.hyperparameters.getProperty("name", "Relex Vectors");
		ef.items = words;
		ef.vectors = vectors;
		ef.write(finalEmbeddings);
	}
	
	/**
	 * puts the collection into a sorted array
	 * @param strs
	 * @return
	 */
	public static String[] toArray(Collection<String> strs) {
	    List<String> strList = new ArrayList<>(strs);
        Collections.sort(strList);
        return strList.toArray(new String[strList.size()]);
	}
	
	/**
	 * Loads the vocabulary of the EmbeddingFormat file.
	 * @param f
	 * @return
	 */
	public static Map<String,Integer> getWordNdx(File f) {
		return new EmbeddingFormat(f, true).getItem2Ndx();
	}
	
	/**
	 * Load the map from entity-id to type vector, used when we have vector valued types rather than discrete types with embeddings.
	 * @param typeVector
	 * @return
	*/
	public static Map<String,float[]> readTypeVectors(File typeVector) {
		Map<String,float[]> id2TypeVector = new HashMap<>();
		for (File f : new FileUtil.FileIterable(typeVector))
			for (String[] parts : new SimpleTsvIterable(f)) {
				if (parts.length != 2) {
					throw new Error("bad line in "+f.getAbsolutePath()+": "+Lang.stringList(parts, "\t"));
				}
				id2TypeVector.put(parts[0], DenseVectors.toFloatArray(DenseVectors.fromString(parts[1])));
			}
		return id2TypeVector;
	}
	
	/**
	 * Example Args:
	   
	   /baseDir/simpleFormatTsvDir 
	   /baseDir/fromWord2VecConverter/wordVectors.ef 
	   /baseDir/tensorDir
 
	 * @param args
	 */
	public static void main(String[] args) {
		String tsvDir = args[0];
		String initialEmbeddings = args[1];
		String outDir = args[2];
		
		//TODO: get from RelexConfig
		int vocabLimit = 100000;
		int minCount = 2;
		
		Pipeline tokenize = new Pipeline(new ClearNLPTokenize());
		tokenize.initialize(new Properties());
		RelexVocab total = new RelexVocab();
		for (File f : new FileUtil.FileIterable(new File(tsvDir))) {
			for (RelexMention m : RelexMentionReader.getReader(f, RelexMention.class)) {
				RelexVocab v = new RelexVocab(m, tokenize);
				total.merge(v, vocabLimit);
			}
		}
		total.removeBelowMinCount(minCount);
		
		RelexVocab.writeVocab(
				new File(initialEmbeddings), 
				new File(outDir, RelexDatasetFiles.wordVectors), 
				total.word2count.keySet(), 
				new String[0]);
		RelexConfig config = new RelexConfig();
		config.entityTypes = toArray(total.getTypes());
		config.relationTypes = toArray(total.getRelations());
		config.vocabMinCount = minCount;
		config.vocabLimit = total.word2count.size();
		FileUtil.writeFileAsString(new File(outDir, "relexConfig.properties"), config.toString());

	}
}