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

import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;

public class FilterEntsByGroundTruth implements IPostprocessEntityRecognition {
	private static final long serialVersionUID = 1L;

	protected Set<String> relevantUrls;
	
	@Override
	public void initialize(Properties config) {}

	@Override
	public void process(Document doc) {
		doc.removeAnnotations(EntityWithId.class, e -> !relevantUrls.contains(e.id));
	}

	@Override
	public void initialize(IGroundTruth gt, RelexConfig config) {
		this.relevantUrls = gt.getRelevantIds();
	}

}
