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
package com.ibm.reseach.ai.ki.nlp.types;

import java.util.*;

import com.fasterxml.jackson.annotation.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

public class LinkedEntity extends Entity {
	private static final long serialVersionUID = 1L;
	
	public float confidence; // confidence of the entity span - not the links
	protected List<Pair<String,Double>> linkedTo;
	
	public List<Pair<String,Double>> getLinkedTo() {
		return Lang.NVL(linkedTo, Collections.EMPTY_LIST);
	}
	
	//CONSIDER: this could share an abstract base class with EntityWithId
	public String getMostLikelyLinkedTo() {
		if (linkedTo == null)
			return null;
		double maxScore = Double.NEGATIVE_INFINITY;
		String ent = null;
		for (Pair<String,Double> p : linkedTo) {
			if (p.second > maxScore) {
				ent = p.first;
				maxScore = p.second;
			}
		}
		return ent;
	}
	
	public void addLinkedTo(String entity, double confidence) {
		if (linkedTo == null)
			linkedTo = new ArrayList<>();
		linkedTo.add(Pair.of(entity, confidence));
	}
	
	public void setLinkedTo(List<Pair<String,Double>> linkedTo) {
		this.linkedTo = linkedTo;
	}
	
	@JsonCreator
	public LinkedEntity(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end, @JsonProperty("type") String type) {
		super(source, start, end, type);
	}	

	//testing that json serialization works
	/*
	public static void main(String[] args) {
	    LinkedEntity le = new LinkedEntity("T", 0, 10, "testType");
	    le.addLinkedTo("test1", 0.4);
	    le.addLinkedTo("test2", 0.7);
	    Document d = new Document("Gooble dooble froogle. This is a test document.");
	    d.addAnnotation(le);
	    String json = DocumentJSONSerializer.toJSON(d);
	    System.out.println(json);
	    Document dj = DocumentJSONDeserializer.fromJSON(json);
	    System.out.println(dj.toSimpleInlineMarkup());
	    LinkedEntity lej = dj.getSingleAnnotation(LinkedEntity.class);
	    for (Pair<String,Double> p : lej.getLinkedTo())
	        System.out.println(p);
	}
	*/
}
