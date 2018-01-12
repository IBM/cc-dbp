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

import com.ibm.research.ai.ki.util.*;

/**
 * Configuration for the construction of a Relational Knowledge Induction dataset.
 * @author mrglass
 *
 */
public class RelexConfig extends PropertyStruct {
	private static final long serialVersionUID = 1L;

	//CONSIDER:
	//try with argument placeholders
	//  some flag in the tensordataset creation; 
	//  RelexConfig.argumentPlaceholder
	//  RelexConfig.groupByTypesAndRelations
	
	public enum TypeStyle {
		/**
		 * no type information
		 */
		none,
		/**
		 * each entity has a single discrete type
		 */
		single,
		/**
		 * each entity has a single discrete type per mention
		 */
		avgOverMentions,
		/**
		 * each entity has a vector representing its type
		 */
		vector
	}	
	
	
	public enum DirectionStyle {
		/**
		 * Each entity pair is added as id_a,id_b and id_b,id_a with the ground truth relations reversed.
		 * Relation direction is indicated by a '<' or '>' prefix.
		 */
		bothWays,
		/**
		 * The direction of the relation is ignored.
		 */
		ignore,
		/**
		 * The direction is always forward, depending on the ground truth, this may not be possible
		 */
		fixed
	}
	
	/**************************************
	 * Used in tsv dataset construction and evaluation
	 **************************************
	 */
	
	/**
	 * Percent negative actually included. This is populated by the dataset generation. 
	 * It will be different from negativeExampleSampleFraction if targetNegativeToPositiveRatio is set.
	 */
	public double retainNegativeProb = 1.0;
	
	/**************************************
	 * Used in tsv dataset construction
	 **************************************
	 */
	
	/**
	 * The fraction of documents in the document collection to use. Mainly for creating smaller datasets when doing pilot experiments.
	 */
	public double documentSampleFraction = 1.0;
	
	/**
	 * The fraction of negative examples to include in the dataset. 1.0 for all, 0.05 for 5%.
	 */
	public double negativeExampleSampleFraction = 1.0;
	
	/**
	 * If set, we use negativeExampleSampleFraction for the first pass, then downsample further to match this ratio.
	 */
	public double targetNegativeToPositveRatio = Double.NaN;
	
	/**
	 * consider the relations with the arguments both forward and reversed
	 */
	public DirectionStyle directionStyle = DirectionStyle.ignore;
	/**
	 * include the title of the document as context, and possible source of entity mentions for each sentence
	 */
	public boolean titleContext = true;
	/**
	 * include all section headers (hierarchically) as context, and possible source of entity mentions for each sentence
	 */
	public boolean sectionContext = true;

	/**
	 * if greater than zero, mentions for the tsv dataset are based on a token window
	 */
	public int mentionTokenWindowSize = -1;
	
	/**
	 * Only construct mentions where we have both entities in the GroundTruth.relevantUrls
	 */
	public boolean limitEntitiesToGroundTruth = false;
	
	/**
	 * List of class names implementing IPostprocessEntityRecognition
	 */
	public String[] entityRecognitionPostProcess = null;
	
	/**
	 * Get types from the GroundTruth, otherwise will get type from the EntityWithId
	 */
	public boolean gtTypes = false;
	
	/**
	 * The name of a class implementing IEntityPairFilter.
	 * The name is specific to binary relex, but we use it for IEntityFilter in unary too.
	 */
	public String entityPairFilterClass;
	
	/**
	 * minimum length of sentence for constructing mentions
	 */
	public int minSentenceChars = 2;
	
	/**
	 * maximum length of sentence for constructing mentions
	 * including sections and title if they are used as context
	 */
	public int maxSentenceChars = 400;

	/**
	 * minimum length of sentence for constructing mentions
	 */
	public int minSentenceTokens = 2;
	
	/**
	 * maximum length of sentence for constructing mentions
	 * including sections and title if they are used as context
	 * CONSIDER: rather than ignoring too-long sentences, maybe just take the tokens in a window around the term(s)
	 */
	public int maxSentenceTokens = 100;	
	
	/**
	 * Gather statistics when building the dataset
	 */
	public boolean gatherRelexStats = true;
	
	/**
	 * The filename for the serialized tokenizer pipeline. If not set it defaults to new File(convertDir, RelexDatasetFiles.tokenizer).
	 * If that file doesn't exist, it defaults to ClearNLPTokenize.
	 */
	public String tokenizerPipelineFile = null;
	//CONSIDER: max token gap
	
	/**************************************
	 * Used in vocabulary construction
	 **************************************
	 */
	
	/**
	 * Maximum number of words to have embeddings for.
	 */
	public int vocabLimit = 1000000;
	
	/**
	 * Minimum number of times word should occur in dataset for it to be in the vocab.
	 */
	public int vocabMinCount = 2;
	
	/**
	 * The embeddings are initialized with the values from this EmbeddingFormat file.
	 */
	public String initialEmbeddingsFile;
	
	/**
	 * whether to save the extracted vocabulary in an RDD (or just in the embeddings format file)
	 */
	public boolean saveVocabRDD = false;
	
	/**
	 * Location of the word2vec model; convertDir/wordVectors.ef if null
	 */
	public String word2vecModelFile;
	
	/**************************************
	 * Used in tensor dataset construction
	 **************************************
	 */
	
	/**
	 * For constructing learning curves. We build the whole tsv dataset + vocab. 
	 * Then limit mentions by document when constructing the tensor dataset
	 */
	public double documentFractionForTensorDataset = 1.0;
	
	/**
	 * Minimum number of mentions for an entity pair to be included in the dataset.
	 */
	public int minMentionSet = 1;
	
	/**
	 * Maximum number of mentions to consider for a single entity pair.
	 * Mention sets larger than this are split.
	 */
	public int maxMentionSet = 100;
	
	/**
	 * When an entity-pair has more mentions than maxMentionSet, these mentions are split into groups.
	 * This limits the number of groups; groups greater than this will be discarded.
	 */
	public int maxMentionGroups = 10;
	
	/**
	 * The number of rows in the position embeddings matrix.
	 */
	public int maxPositionEmbeddings = 80;
	
	/**
	 * How type information is incorporated.
	 */
	public TypeStyle typeStyle = TypeStyle.single;
	
	/**
	 * If non-null, vectors describing the type of each entity-id are in this file.
	 */
	public String entityTypeVectorFile = null;

	/**
	 * The number of binary tensor set instances per file. Easier index-free shuffling with lower numbers.
	 * Easier random access indexing with larger number.
	 */
	public int tensorInstancesPerFile = 10000;
	
    /**************************************
     * For apply and triple extract
     **************************************
     */	
	
	//TODO: maybe have location of trained word vectors, model, hypers and id files?
	// or just have it be that wordVectorsTrained.ef, hypers.properties and model.bin are the standard filenames in the convertDir
	
	/**
	 * The minimum score the model gives a triple for it to be included in the output
	 */
	public double minConfidenceScore = 0;
	
	/**
	 * Maximum number of supporting sentences for an extracted triple
	 */
	public int maxSupportingMentions = 10;
	
	/**************************************
	 * Used in multiple stages
	 **************************************
	 */
	
	/**
	 * The name of the class to provide implementations for IRelexMention, tsv and tensor dataset construction
	 */
	public String relexManagerClass = RelexDatasetManagerBinary.class.getCanonicalName();
	
	protected transient IRelexDatasetManager _rmanager;
	public synchronized IRelexDatasetManager<?> getManager() {
	    try {
	        if (_rmanager == null) {
	            _rmanager = (IRelexDatasetManager)Class.forName(relexManagerClass).newInstance();
	            _rmanager.initialize(this);
	        }
	        return _rmanager;
	    } catch (Exception e) {
	        throw new Error(e);
	    }
	}
	
	/**
	 * The GroundTruth serialized in a file used to construct the dataset.
	 */
	public String groundTruthFile;
	
	/**
	 * The directory used for the files built by tsv to tensor conversion
	 */
	public String convertDir;
	
	/**
	 * The names of the dataset splits
	 */
	public String[] datasetSplitNames = new String[] {"train", "validate", "test"};
	/**
	 * The fraction of the dataset that goes into each split
	 */
	public double[] datasetSpitFractions = new double[] {0.9, 0.05, 0.05};
	
	/**
	 * Names of the relations, populated by dataset construction
	 */
	public String[] relationTypes;

	/**
	 * Names of the entity types, populated by dataset construction
	 */
	public String[] entityTypes;
	
	//simple test
	public static void main(String[] args) {
		System.out.println(new RelexConfig().toString());
		RelexConfig c = new RelexConfig();
		c.fromString(new RelexConfig().toString());
		System.out.println(c.toString());
	}
	
}
