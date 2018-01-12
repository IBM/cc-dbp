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

import com.ibm.reseach.ai.ki.nlp.*;

/**
 * Creates the deep learning input tensors from a set of RelexMentions
 * @author mrglass
 *
 * @param <M>
 */
public interface IRelexTensors<M extends IRelexMention> extends Serializable {
	public String[] getTypes();
	public String[] getRelations();
	/**
	 * The first object is assumed to be the String groupId.
	 * @param tokenizer
	 * @param fullMentionSet
	 * @return
	 */
	public List<Object[]> makeInstances(Annotator tokenizer, Collection<M> fullMentionSet);
}
