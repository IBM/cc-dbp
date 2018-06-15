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

import com.ibm.reseach.ai.ki.nlp.*;

/**
 * So we can unify the code for binary and unary relation mention
 * @author mrglass
 *
 */
public interface IRelexMention extends Serializable {
	//for reduce by key
	public String groupId();
	public int groupSplit(int splitCount);
	
	//the canonically ordered list of ids, separated by '\t'; if group ids are enabled the group id is given here too
	public String entitySetId();
	
	//downsampling and splitting train/validate/test
	public double getNegativeDownsamplePriority();
	public double getDatasetSplitPosition();
	//for negative downsampling
	public boolean isNegative();
	
	//where the document the mention comes from appears in the x-axis of the document learning curve (0-1)
	public double getDocumentLearningCurvePosition();
	
	//for vocab construction
	public String[] getTypes();
	public String[] getRelations();
	public String[] getTokens(Annotator tokenizer);
	
	//saving and loading from tsv
	public void fromString(String tsvLine);
	
	public String toString();
	
	//to avoid duplicates in a mentionset, if non-null, two IRelexMentions that share a uniquenessString are duplicates.
	public String uniquenessString();
	
	/**
	 * A human readable format for showing the support for an extracted relation.
	 * @return
	 */
	public String toSupportString();
	
	public void convertToPlaceholders();
}
