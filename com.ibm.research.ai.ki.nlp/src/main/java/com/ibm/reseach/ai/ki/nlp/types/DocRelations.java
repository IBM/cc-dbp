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

import java.io.*;
import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;

/**
 * NOTE: doesn't json serialize
 * @author mrglass
 *
 */
public class DocRelations extends ArrayList<DocRelations.Relation> implements DocumentStructure {
	private static final long serialVersionUID = 1L;
	
	protected Document doc;
	
	private DocRelations(Document doc) {
		this.doc = doc;
	}
	
	public static DocRelations getDocRelations(Document doc) {
		DocRelations dr = doc.getDocumentStructure(DocRelations.class);
		if (dr == null) {
			dr = new DocRelations(doc);
			doc.setDocumentStructure(dr);
		}
		return dr;
	}
	
	public static void convertToMentions(Document doc) {
		DocRelations dr = doc.getDocumentStructure(DocRelations.class);
		if (dr != null)
			for (DocRelations.Relation r : dr)
				doc.addAnnotation(r.toRelationMention());
	}
	
	public static void addToCorefIndex(Document doc) {
		CorefIndex cindex = CorefIndex.getCorefIndex(doc);
		DocRelations dr = doc.getDocumentStructure(DocRelations.class);
		if (dr != null)
			for (DocRelations.Relation r : dr) {
				cindex.addChain(r.arg1);
				cindex.addChain(r.arg2);
			}
	}
	
	public void addRelation(String relType, CorefIndex.Chain arg1, CorefIndex.Chain arg2) {
		this.add(new Relation(relType, arg1, arg2));
	}
	
	public class Relation implements Serializable {
		private static final long serialVersionUID = 1L;
		public String relType;
		public CorefIndex.Chain arg1;
		public CorefIndex.Chain arg2;
		//TODO: Map<String,MutableDouble> features as well?
		
		//CONSIDER: validate that the CorefIndex.Chain is present in the Document.getDocumentStructure?
		private Relation(String relType, CorefIndex.Chain arg1, CorefIndex.Chain arg2) {
			this.relType = relType;
			this.arg1 = arg1;
			this.arg2 = arg2;
		}
		
		//convert to plain Relation - by just picking first from each coref chain
		//CONSIDER: instead pick the two closest mentions?
		public com.ibm.reseach.ai.ki.nlp.types.Relation toRelationMention() {
			Annotation a1 = arg1.getFirst(Annotation.class);
			Annotation a2 = arg2.getFirst(Annotation.class);
			if (a1 == null || a2 == null)
				throw new Error();
			return new com.ibm.reseach.ai.ki.nlp.types.Relation(DocRelations.Relation.class.getSimpleName(), doc, a1, a2, relType);
		}
		
	}
}
