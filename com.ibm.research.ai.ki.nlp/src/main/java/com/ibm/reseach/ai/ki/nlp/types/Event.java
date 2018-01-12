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

/**
 * The span of the Event is something like the event extent and is usually not very meaningful.
 * Most of the semantics come from the eventType and the argument mentions and roles.
 * @author mrglass
 *
 */
public class Event extends Annotation {
	private static final long serialVersionUID = 1L;
	
	public Event(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end, @JsonProperty("type") String type) {
		super(source, start, end);
		this.type = type;
	}

	public void addArgument(Document doc, String role, Entity entity) {
		arguments.add(Pair.of(role, doc.getAnnoRef(entity)));
	}
	
	//the type of the event
	public String type;
	
	//CONSIDER: maybe allow arguments to be Annotation in general rather than requiring Entity
	//Pair is role name and entity, role name may be null
	public List<Pair<String,AnnoRef<Entity>>> arguments = new ArrayList<>();
	
	//CONSIDER: pull the trigger into an EventTrigger annotation class?
	//the span of the trigger, may be null
	public Span trigger;

	@Override
	public String highlightLabel() {
		return type;
	}
}
