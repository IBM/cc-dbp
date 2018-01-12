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
import java.util.stream.*;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;

/**
 * Annotation describing a list in text
 * @author mrglass
 *
 */
public class ListAnnotation extends Annotation {
	private static final long serialVersionUID = 1L;
	
	@JsonCreator
	public ListAnnotation(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end) {
		super(source, start, end);
	}

	public List<AnnoRef<ListItem>> items = new ArrayList<>();
	
	public void addListItem(Document doc, ListItem item) {
		doc.addAnnotation(item);
		items.add(doc.getAnnoRef(item));
	}
	
}
