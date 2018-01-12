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

/**
 * A class implementing this will be specified in the RelexConfig if some filtering of entity-pairs is desired.
 * Otherwise the tsv dataset will contain all pairs of EntityWithId that occur in the same sentence.
 * @author mrglass
 *
 */
public interface IEntityPairFilter extends Serializable {
    /**
     * In Spark, initialize is called in the Spark head
     * @param gt
     * @param config
     */
	public void initialize(GroundTruth gt, RelexConfig config);
	/**
	 * Return true if the entity-pair is a good candidate
	 * @param id1
	 * @param type1
	 * @param id2
	 * @param type2
	 * @return
	 */
	public boolean test(String id1, String type1, String id2, String type2);
}
