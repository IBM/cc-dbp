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
package com.ibm.research.ai.ki.kbp.unary;

import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;


/**
 * Idea is to gather the contexts that an entity occurs in, then use sequences->vectors then aggregate over the sequence output vectors.
 * So we are learning to determine the unary-relation/type of a term from its mention set.
 * If we end up doing a large number of types then we may predict a type embedding vector rather than a vector of length = #types.
 * We can augment this with relation extractors specialized for isa and co-hyponym 
 * (maybe an anti-co-hyponym relex? like some pattern that indicates two terms are far in the taxonomy?)
 * 
 * Possible unary relations: company-in-industry, citizen-of-country, politician-in-party, occupation, person-has-gender
 * Basically any relation where one argument is drawn from a relatively small set.
 * @author mrglass
 *
 */
public class UnaryRelexMention implements IRelexMention {
	private static final long serialVersionUID = 1L;
	
	public String id;
	public String type;
	
	public Span span;
	public String sentence;
	
	public String[] relations;
	
	public String documentId;
	public Span textSpan;
	
	public UnaryRelexMention() {}
	
	protected void setFields(String[] tsvParts) {
	    id = tsvParts[0];
        type = tsvParts[1];
        span = Span.fromString(tsvParts[2]);
        relations = tsvParts[3].isEmpty() ? UnaryGroundTruth.noRelations : tsvParts[3].split(",");
        
        sentence = tsvParts[4];	 
        if (tsvParts.length > 5)
            documentId = tsvParts[5];
        if (tsvParts.length > 6)
            textSpan = Span.fromString(tsvParts[6]);
	}
	
	
	public UnaryRelexMention(String id, String type, Span span, String[] relations, String sentence, String documentId, Span textSpan) {
		this.id = id;
		this.type = type;
		this.span = span;
		this.relations = Lang.NVL(relations, UnaryGroundTruth.noRelations);
		this.sentence = sentence;
		this.documentId = documentId;
		this.textSpan = textSpan;
	}
	
	public String toString() {
		return id+"\t"+type+"\t"+span.toString()+"\t"+Lang.stringList(relations, ",")+"\t"+
				sentence.replace('\t', ' ').replace('\n', ' ')+
				(documentId != null ? "\t"+documentId : "")+
				(textSpan != null ? "\t"+textSpan.toString() : "");
	}

	@Override
	public String groupId() {
		return id;
	}

	public int groupSplit(int splitCount) {
	    int splitNdx = (int)Math.floor(RandomUtil.pseudoRandomFromString("GS:"+this.sentence)*splitCount);
        if (splitNdx >= splitCount)
            splitNdx = splitCount - 1;
        return splitNdx;
	}
	
	public static final String ARGPLACEHOLDER = "THEARG";
	
	public void convertToPlaceholders() {
	    String p1 = sentence.substring(0, span.start);
	    String p2 = sentence.substring(span.end, sentence.length());
	    span.end = span.start + ARGPLACEHOLDER.length();
	    sentence = p1 + ARGPLACEHOLDER + p2;
	}
	
	@Override
	public String entitySetId() {
		return id;
	}
	
	@Override
	public double getNegativeDownsamplePriority() {
		return GroundTruth.getSingleDownsamplePriority(id);
	}

	@Override
	public double getDatasetSplitPosition() {
		return GroundTruth.getSingleSplitLocation(id);
	}

    @Override
    public double getDocumentLearningCurvePosition() {
        if (documentId == null)
            throw new UnsupportedOperationException();
        return new Random(documentId.hashCode() + 123321).nextDouble();
    }
	
	@Override
	public boolean isNegative() {
		return relations.length == 0;
	}

	@Override
	public String[] getTypes() {
		return new String[] {type};
	}

	@Override
	public String[] getRelations() {
		return relations;
	}

	@Override
	public String[] getTokens(Annotator tokenizer) {
		Document d = new Document(sentence);
		tokenizer.process(d);
		String[] toks = d.toStringArray(d.getAnnotations(Token.class));
		//RelexVocab will handle this
		//for (int i = 0; i < toks.length; ++i)
		//	toks[i] = RelexVocab.normalized(toks[i]);
		return toks;
	}

	@Override
	public void fromString(String tsvLine) {
		String[] tsvParts = tsvLine.split("\t");
		setFields(tsvParts);
	}

	@Override
	public String uniquenessString() {
		return span.toString()+"\t"+sentence;
	}
	
    @Override
    public String toSupportString() {
        String escapeSent = sentence.replace('\n', ' ').replace('\t', ' ').replace("<<<", "   ").replace(">>>", "   ");
        return Span.highlightAll(escapeSent, Arrays.asList(span), "<<<", ">>>");
    }
    
    public Pair<String,Span> getProvenance() {
        return Pair.of(documentId, textSpan);
    }
}
