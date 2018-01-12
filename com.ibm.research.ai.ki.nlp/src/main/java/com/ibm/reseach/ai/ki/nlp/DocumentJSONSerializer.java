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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.fasterxml.jackson.databind.ser.std.*;

import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

import java.util.*;

/**
 * JSON serializes Documents and most Annotations and DocumentStructures.
 * Use FileUtil.objectToBase64String when full, reliable serialization to a string (like for spark) is needed.
 * 
 * @author mrglass
 *
 */
public class DocumentJSONSerializer extends StdSerializer<Document> {
	private static final long serialVersionUID = 1L;

	public DocumentJSONSerializer() {
		this(Document.class);
	}
	protected DocumentJSONSerializer(Class<Document> t) {
		super(t);
	}
	
	static final String standardTypePackage = Token.class.getPackage().getName()+".";
	static String serializedClassName(Class c) {
		String name = c.getCanonicalName();
		if (name.startsWith(standardTypePackage))
			name = name.substring(standardTypePackage.length());
		return name;
	}
	
    @Override
    public void serialize(Document doc, JsonGenerator jgen, SerializerProvider provider) throws IOException,
            JsonGenerationException {
        jgen.writeStartObject();
        jgen.writeStringField("id", doc.id);
        jgen.writeStringField("text", doc.text);
        // we record only the most specific class for each annotation
        Map<Class, ArrayList<Annotation>> annos = new HashMap<>();
        Map<Annotation, Integer> ids = new HashMap<>();
        for (Annotation a : doc.getAnnotations(Annotation.class)) {
            HashMapUtil.addAL(annos, a.getClass(), a);
            ids.put(a, ids.size());
        }
        // go through all annorefs and set the ids
        AnnoRef.JSONSerializer.startAnnoRefIds(ids);
        if (!annos.isEmpty()) {
            jgen.writeFieldName("annotations");
            jgen.writeStartObject();
            for (Map.Entry<Class, ArrayList<Annotation>> e : annos.entrySet()) {
                jgen.writeArrayFieldStart(serializedClassName(e.getKey()));
                for (Annotation a : e.getValue()) {
                    jgen.writeStartObject();
                    jgen.writeNumberField("id", ids.get(a));
                    jgen.writeObjectField("anno", a);
                    jgen.writeEndObject();
                }
                jgen.writeEndArray();
            }
            jgen.writeEndObject();
        }

        // then we'll do the document structures
        if (doc.docLevelStructures != null && !doc.docLevelStructures.isEmpty()) {
            jgen.writeFieldName("structures");
            jgen.writeStartObject();
            for (Map.Entry<Class, DocumentStructure> e : doc.docLevelStructures.entrySet()) {
                if (e.getKey() == CorefIndex.class) {
                    jgen.writeArrayFieldStart(serializedClassName(e.getKey()));
                    for (CorefIndex.Chain chain : (CorefIndex)e.getValue()) {
                        int[] annoIds = new int[chain.size()];
                        int ndx = 0;
                        for (Annotation a : chain) {
                            annoIds[ndx++] = ids.get(a);
                        }
                        jgen.writeObject(annoIds);
                    }
                    jgen.writeEndArray();
                } else {
                    jgen.writeObjectField(serializedClassName(e.getKey()), e.getValue());
                }
            }
            jgen.writeEndObject();
        }
        
        // ?? provider.defaultSerializeValue(arg0, arg1);
        // jgen.writeObjectField(fieldName, pojo);
        jgen.writeEndObject();
        AnnoRef.JSONSerializer.finishAnnoRefIds();
    }

	static ObjectMapper mapper;
	static {
		mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
        SimpleModule mod = new SimpleModule();
        mod.addSerializer(Document.class, new DocumentJSONSerializer());
        mod.addSerializer(AnnoRef.class, new AnnoRef.JSONSerializer());
        mod.addDeserializer(Document.class, new DocumentJSONDeserializer());
        mod.addDeserializer(AnnoRef.class, new AnnoRef.JSONDeserializer());
        mapper.registerModule(mod);
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE));
	}
	
	public static String toJSON(Document doc) {
		try {
			return mapper.writeValueAsString(doc);
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	// http://www.baeldung.com/jackson-deserialization
	// http://www.davismol.net/2015/05/18/jackson-create-and-register-a-custom-json-serializer-with-stdserializer-and-simplemodule-classes/
	// http://www.mkyong.com/java/jackson-2-convert-java-object-to-from-json/
	// http://blog.palominolabs.com/2012/06/05/writing-a-custom-jackson-serializer-and-deserializer/
	
	/**
	 * Example args:
	 * 
	 * wikipedia/enwikidoc
	 * 
	 * OR
	 * 
	 * wikipedia/wikidocParsed
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		DocumentReader read = new DocumentReader(new File(args[0]));
		Document doc = read.iterator().next();
		
		String jsonInString = toJSON(doc);
		System.out.println(doc.toSimpleInlineMarkup());
		System.out.println(jsonInString);
		Document doc2 = DocumentJSONDeserializer.fromJSON(jsonInString);
		System.out.println(doc2.toSimpleInlineMarkup());
	}
}
