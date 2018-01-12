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
package com.ibm.research.ai.ki.util.eval;

import java.util.*;

import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.eval.PrecisionRecall.*;

public class MultiPrecisionRecall {
	public static final String ALL = "ALL";
	
	public Map<String, PrecisionRecall> prs;
	
	public MultiPrecisionRecall() {
		prs = new HashMap<>();
		prs.put(ALL, new PrecisionRecall());
	}
	
	public void addAnswered(String id, double score, boolean relevant, double weight, String... tags) {
		Instance inst = new Instance(id, score, relevant, weight);
		for (String t : tags) {
			if (t == null)
				continue;
			if (t.equals(ALL))
				throw new IllegalArgumentException("The tag '"+ALL+"' is reserved");
			//CONSIDER: check for duplicate tags?
			prs.computeIfAbsent(t, s -> new PrecisionRecall()).addAnswered(inst);
		}
		prs.get(ALL).addAnswered(inst);
	}
	
	public void addOutOfRecall(int outOfRecallCount, String... tags) {
		for (String t : tags) {
			if (t.equals(ALL))
				throw new IllegalArgumentException("The tag '"+ALL+"' is reserved");
			//CONSIDER: check for duplicate tags?
			prs.computeIfAbsent(t, s -> new PrecisionRecall()).addOutOfRecall(outOfRecallCount);
		}
		prs.get(ALL).addOutOfRecall(outOfRecallCount);
	}
	
	public Map<String,PrecisionRecall.SummaryScores> computeSummaryScores() {
		return Maps.transformValues(prs, pr -> pr.computeSummaryScores());
	}
	
}
