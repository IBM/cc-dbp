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
package com.ibm.research.ai.ki.nlp;

import java.util.*;

import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

public class OverlappingSpansTest {
	public void validate(Document doc) {
		OverlappingSpans ospans = new OverlappingSpans(doc.getAnnotations(Annotation.class));
		List<Annotation> sample = RandomUtil.getSample(doc.getAnnotations(Annotation.class), 100);
		for (Annotation a : sample) {
			validate(ospans, doc, a);
		}
	}
	protected void validate(OverlappingSpans ospans, Document doc, Annotation a) {
		Set<Span> ores = ospans.getSpansOverlapping(a);
		Set<Span> linearRes = new HashSet<>();
		for (Annotation oa : doc.getAnnotations(Annotation.class)) {
			if (oa.overlaps(a)) 
				linearRes.add(oa);
		}
		int matchSize = Sets.intersection(ores, linearRes).size();
		if (matchSize != ores.size())
			throw new Error("fail");
	}
}
