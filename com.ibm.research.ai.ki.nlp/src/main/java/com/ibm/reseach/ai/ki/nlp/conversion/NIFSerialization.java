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
package com.ibm.reseach.ai.ki.nlp.conversion;

import java.io.*;
import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

import org.apache.commons.lang3.*;

/**
 * Handles the subset of NIF used in OKE challenge. https://project-hobbit.eu/open-challenges/oke-open-challenge/
 * @author mrglass
 *
 */
public abstract class NIFSerialization {
    static class NIFStatementGroup {
        String nifId;
        int start;
        int end;
        String ref;
        String text;
        String linkId;
    }
    
    static Map<String,Document> getDocuments(Collection<NIFStatementGroup> nifStatements) {
        Map<String, Document> docs = new HashMap<>();
        for (NIFStatementGroup ng : nifStatements) {
            if (ng.ref == null) {
                //this is a document, create it
                docs.put(ng.nifId, new Document(ng.nifId, ng.text));
            }
        }
        for (NIFStatementGroup ng : nifStatements) {
            if (ng.ref != null) {
                //this is an EntityWithId, add to proper document
                Document doc = docs.get(ng.ref);
                if (doc == null) {
                    throw new Error("not found: "+ng.ref);
                }
                EntityWithId eid = new EntityWithId(null, ng.start, ng.end, "NIF", ng.linkId);
                doc.addAnnotation(eid);
            }
        }
        return docs;
    }
    
    public static Map<String,Document> fromNIF(String nifDoc) {
        return fromNIF(new BufferedReader(new StringReader(nifDoc)));
    }
    
    static String unbracket(String str) {
        if (str.startsWith("<") && str.endsWith(">"))
            return str.substring(1, str.length()-1);
        return str;
    }
    
    public static Map<String,Document> fromNIF(BufferedReader reader) {
        String line = null;
        NIFStatementGroup ng = null;
        Collection<NIFStatementGroup> nifs = new ArrayList<>();
        try {
            while ((line = reader.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }
                if (line.startsWith("<")) {
                    if (ng != null) {
                        nifs.add(ng);
                    }
                    ng = new NIFStatementGroup();
                    ng.nifId = unbracket(line);
                } else {
                    String[] parts = line.trim().split("\\s+");
                    if (parts[0].equals("nif:beginIndex")) {
                        ng.start = Integer.parseInt(line.substring(line.indexOf('"')+1, line.lastIndexOf('"')));
                    } else if (parts[0].equals("nif:endIndex")) {
                        ng.end = Integer.parseInt(line.substring(line.indexOf('"')+1, line.lastIndexOf('"')));
                    } else if (parts[0].equals("nif:isString")) {
                        ng.text = StringEscapeUtils.unescapeJava(line.substring(line.indexOf('"')+1, line.lastIndexOf('"')));
                    } else if (parts[0].equals("nif:referenceContext")) {
                        ng.ref = unbracket(parts[1]);
                    } else if (parts[0].equals("itsrdf:taIdentRef")) {
                        ng.linkId = unbracket(parts[1]);
                    }
                }
            }
            if (ng != null) {
                nifs.add(ng);
            }
        } catch (IOException e) {
            throw new Error(e);
        }
        return getDocuments(nifs);
    }
    
    public static String toNIF(Document doc) {
        StringBuilder buf = new StringBuilder();
        
        String docBaseId = doc.id;
        if (docBaseId.endsWith("#char=0,"+doc.text.length())) {
            docBaseId = doc.id.substring(0, doc.id.lastIndexOf("#"));
        }
        
        buf.append("<"+doc.id+">\n");
        buf.append("        a               nif:RFC5147String , nif:String , nif:Context ;\n");
        buf.append("        nif:beginIndex  \"0\"^^xsd:nonNegativeInteger ;\n");
        buf.append("        nif:endIndex    \""+doc.text.length()+"\"^^xsd:nonNegativeInteger ;\n");
        buf.append("        nif:isString    \""+StringEscapeUtils.escapeJava(doc.text)+"\"^^xsd:string .\n\n");
        
        for (EntityWithId eid : doc.getAnnotations(EntityWithId.class)) {
            buf.append("<"+docBaseId+"#char="+eid.start+","+eid.end+">\n");
            buf.append("        a                     nif:RFC5147String , nif:String , nif:Phrase ;\n");
            buf.append("        nif:anchorOf          \""+StringEscapeUtils.escapeJava(doc.coveredText(eid))+"\"^^xsd:string ;\n");
            buf.append("        nif:beginIndex        \""+eid.start+"\"^^xsd:nonNegativeInteger ;\n");
            buf.append("        nif:endIndex          \""+eid.end+"\"^^xsd:nonNegativeInteger ;\n");
            buf.append("        nif:referenceContext  <"+doc.id+"> ;\n");
            buf.append("        itsrdf:taIdentRef     <"+eid.id+"> .\n\n");
        }
        
        return buf.toString();
    }
    
    public static void main(String[] args) {
        Collection<Document> docs = fromNIF(FileUtil.readFileAsString("oke17task2Training.xml.ttl")).values();
        for (Document doc : docs) {
            System.out.println(DocumentJSONSerializer.toJSON(doc));
            System.out.println(toNIF(doc));
        }
    }
}
