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
package com.ibm.reseach.ai.ki.nlp;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.module.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Deserializes Documents (with Annotations) written as JSON.
 * CONSIDER: allow annotations not found in classpath to be ignored
 * @author mrglass
 *
 */
public class DocumentJSONDeserializer extends StdDeserializer<Document> {
	private static final long serialVersionUID = 1L;
	
	//NOTE: Annotation subclasses may need to add @JSONProperty to the Annotation constructors
	//see http://www.cowtowncoder.com/blog/archives/2011/07/entry_457.html
	//OR: http://stackoverflow.com/questions/21920367/why-when-a-constructor-is-annotated-with-jsoncreator-its-arguments-must-be-ann/32272061#32272061
	
	 public static Document fromJSON(String json) {
		 try {
			 return DocumentJSONSerializer.mapper.readValue(json, Document.class);
		 } catch (Exception e) {
			 return Lang.error(e);
		 }
	 }
	
	public DocumentJSONDeserializer() {
		this(Document.class);
	}
	public DocumentJSONDeserializer(Class<Document> vc) {
		super(vc);
	}

	@SuppressWarnings("unchecked")
	static Class forName(String cname) {
		try {
			if (cname.indexOf('.') == -1) {
				cname = DocumentJSONSerializer.standardTypePackage + cname;
			}
			return Class.forName(cname);
		} catch (ClassNotFoundException ce) {
			return Lang.error(ce);
		}
	}
	
	@Override
	public Document deserialize(JsonParser jp, DeserializationContext dser) throws IOException, JsonProcessingException {
		try {
			ObjectMapper mapper = (ObjectMapper) jp.getCodec();
			JsonNode node = mapper.readTree(jp);
			String id = node.get("id").asText();
			String text = node.get("text").asText();
			Document doc = new Document(id, text);
			Map<Integer,Annotation> ids = new HashMap<>();
			if (node.has("annotations")) {
    			Iterator<Map.Entry<String,JsonNode>> annoIt = node.get("annotations").fields();
    			AnnoRef.JSONDeserializer.startAnnoRefIds();
    			while (annoIt.hasNext()) {
    				Map.Entry<String,JsonNode> e = annoIt.next();
    				Class<Annotation> annoCls = forName(e.getKey());
    				for (JsonNode aj : e.getValue()) {
    					//this should be an id/annotation pair
    					//  when deserializing the annoref, we should only deserialize the id
    					//  then after all AnnoRefs are read, we just fix the annorefs using the map we built
    					int aid = aj.get("id").asInt();
    					Annotation a = mapper.treeToValue(aj.get("anno"), annoCls);
    					ids.put(aid, a);
    					doc.addAnnotation(a);
    				}
    			}
    			doc.annoRefs = AnnoRef.JSONDeserializer.finishAnnoRefIds(ids);
			}
			
			//document structures
			if (node.has("structures")) {
			    Iterator<Map.Entry<String,JsonNode>> structIt = node.get("structures").fields();
			    while (structIt.hasNext()) {
			        Map.Entry<String,JsonNode> e = structIt.next();
			        Class<? extends DocumentStructure> dsCls = forName(e.getKey());
			        if (dsCls == CorefIndex.class) {
			            CorefIndex ci = CorefIndex.getCorefIndex(doc);
			            for (JsonNode chaini : e.getValue()) {
			                CorefIndex.Chain chain = ci.newChain();
			                for (JsonNode ai : chaini) {
			                    chain.add(ids.get(ai.asInt()));
			                }
			            }			            
			        } else {
			            DocumentStructure docStruct = mapper.treeToValue(e.getValue(), dsCls);
			            doc.setDocumentStructure(docStruct);
			        }
			        
			    }
			}
			
			return doc;
		} catch (Exception e) {
			return Lang.error(e);
		}
	}

}
