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

import com.fasterxml.jackson.annotation.*;

import com.ibm.reseach.ai.ki.nlp.*;

public class Relation extends Annotation {
	private static final long serialVersionUID = 1L;
	
	public String relationType;
	protected AnnoRef<Annotation> arg1;
	protected AnnoRef<Annotation> arg2;
	//CONSIDER: type parameter rather than general 'Annotation'
	public Annotation getArg1() {
		return arg1.get();
	}
	public Annotation getArg2() {
		return arg2.get();
	}
	
	//to support subclasses of Relation that name their arguments
	public String getArg1Name() {
		return "arg1";
	}
	public String getArg2Name() {
		return "arg2";
	}
	
	@JsonCreator
	public Relation(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end) {
		super(source, start, end);
	}

	
	public Relation(String source, Document doc, Annotation arg1, Annotation arg2, String relationType) {
		super(source, Math.min(arg1.start, arg2.start), Math.max(arg1.end, arg2.end));
		this.arg1 = doc.getAnnoRef(arg1);
		this.arg2 = doc.getAnnoRef(arg2);
		this.relationType = relationType;
	}

}
