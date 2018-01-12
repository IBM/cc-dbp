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

public class Entity extends Annotation {
	private static final long serialVersionUID = 1L;
	
	public String type;
	
	@JsonCreator
	public Entity(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end, @JsonProperty("type") String type) {
		super(source, start, end);
		this.type = type;
	}

	public Entity(Entity e) {
		this(e.source, e.start, e.end, e.type);
	}
	
	@Override
	public String highlightLabel() {
		return type;
	}
}
