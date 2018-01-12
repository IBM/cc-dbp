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
import com.fasterxml.jackson.databind.ser.std.*;


public class AnnoRef<T extends Annotation> implements Serializable {
	private static final long serialVersionUID = 1L;

	//package private, construct using Document (which will have a Map<Annotation,AnnoRef>, to support Annotation replacement)
	AnnoRef(T anno) {
		this.anno = anno;
	}
	void set(T anno) {
		this.anno = anno;
	}
	
	public T get() {
		return anno;
	}
	
	private T anno;

	/*
	public static class Unpack<X extends Annotation> implements Function<AnnoRef<X>,X> {
		@Override
		public X apply(AnnoRef<X> input) {
			return input.get();
		}	
	};
	*/
	

	@SuppressWarnings("rawtypes")
    public static class JSONSerializer extends StdSerializer<AnnoRef> {
		private static final long serialVersionUID = 1L;

		//just for JSON serialize/deserialize
		static ThreadLocal<Map<Annotation,Integer>> annoIds = new ThreadLocal<Map<Annotation,Integer>>() {
			@Override
			public Map<Annotation,Integer> initialValue() {
				return new HashMap<>();
			}
		};
		
		public static void startAnnoRefIds(Map<Annotation,Integer> aids) {
			annoIds.set(aids);
		}
		public static void finishAnnoRefIds() {
			annoIds.remove();
		}
		
		public JSONSerializer() {
			this(AnnoRef.class);
		}
		protected JSONSerializer(Class<AnnoRef> t) {
			super(t);
		}
		@Override
		public void serialize(AnnoRef value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
			jgen.writeStartObject();
			Integer id = annoIds.get().get(value.get());
			if (id == null)
				throw new IllegalArgumentException("an annoref linked to an Annotation not indexed in the document!");
	        jgen.writeNumberField("annoId", id);
	        jgen.writeEndObject();
		}
	}
	@SuppressWarnings("rawtypes")
    public static class JSONDeserializer extends StdDeserializer<AnnoRef> {
		public JSONDeserializer() {
			this(AnnoRef.class);
		}
		protected JSONDeserializer(Class<AnnoRef> vc) {
			super(vc);
		}

		private static final long serialVersionUID = 1L;

		//just for JSON serialize/deserialize
		static ThreadLocal<Map<AnnoRef,Integer>> annoIds = new ThreadLocal<Map<AnnoRef,Integer>>() {
			@Override
			public Map<AnnoRef,Integer> initialValue() {
				return new HashMap<>();
			}
		};
		
		public static void startAnnoRefIds() {
			annoIds.get().clear();
		}
		public static Map<Annotation,AnnoRef> finishAnnoRefIds(Map<Integer,Annotation> aids) {
			Map<Annotation,AnnoRef> fordoc = new HashMap<>();
			Map<AnnoRef,Integer> annoId = annoIds.get();
			for (Map.Entry<AnnoRef,Integer> e : annoId.entrySet()) {
				e.getKey().set(aids.get(e.getValue()));
				fordoc.put(e.getKey().get(), e.getKey());
			}
			annoIds.remove();
			return fordoc;
		}
		
		@Override
		public AnnoRef deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
				JsonProcessingException {
			ObjectMapper mapper = (ObjectMapper) jp.getCodec();
			JsonNode node = mapper.readTree(jp);
			AnnoRef ar = new AnnoRef(null);
			annoIds.get().put(ar, node.get("annoId").asInt());
			return ar;
		}
	}
}
