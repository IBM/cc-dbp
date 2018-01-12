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

/**
 * The files saved in the tsv dataset to tensor dataset conversion.
 * Also used when training and applying the model.
 * 
 * @author mrglass
 *
 */
public class RelexDatasetFiles {
    //in the convert dir
	public static final String wordVectors = "wordVectors.ef";
	public static final String groupSplits = "groupSplits.ser.gz";
	public static final String tokenizerPipeline = "tokenizer.ser.gz";
	public static final String typePairFilterFile = "typePairs.tsv";
	public static final String typeFilterFile = "typeUnary.tsv";
	/**
	 * Created by DocEntityStats
	 */
	public static final String idCountsFile = "idCounts.tsv"; 
	
	public static final String dataDirSuffix = "Dir";
	
	//in the hdfsOutputDir
	public static final String hdfsMentions = "relexMentions.tsv";
	public static final String hdfsTensors = "tensors.b64";

}
