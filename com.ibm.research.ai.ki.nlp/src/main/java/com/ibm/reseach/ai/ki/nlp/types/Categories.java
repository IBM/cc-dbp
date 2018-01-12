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

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

/**
 * The categories that a Document belongs to (like Wikipedia categories)
 * @author mrglass
 *
 */
public class Categories extends HashSet<String> implements DocumentStructure {
	private static final long serialVersionUID = 1L;
	/**
	 * add a category to a Document
	 * @param doc
	 * @param category
	 */
	public static void addCategory(Document doc, String category) {
		Categories cats = doc.getDocumentStructure(Categories.class);
		if (cats == null) {
			cats = new Categories();
			doc.setDocumentStructure(cats);
		}
		cats.add(category);
	}
	/**
	 * unmodifiable set of categories
	 * @param doc
	 * @return
	 */
	public static Set<String> getCategories(Document doc) {
		return Collections.unmodifiableSet(Lang.NVL(doc.getDocumentStructure(Categories.class), (Set<String>)Collections.EMPTY_SET));
	}
}
