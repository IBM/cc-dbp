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
package com.ibm.research.ai.ki.kbp;

import java.io.*;
import java.util.*;

import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.FileUtil.*;

import org.apache.commons.lang3.*;

/**
 * Used as an intermediate format. After the preprocessing is done (the mentions of entity pairs are found) we write to a tsv file.
 * We then write that file to tensor input format using ConvertTensorDataset. There will be multiple versions of ConvertTensorDataset,
 * one for each model type. There will also be multiple preprocessing pipelines: Riedel, Diebold, AlchemyAPI, others.
 * @author mrglass
 *
 */
public class RelexMention implements IRelexMention {
	private static final long serialVersionUID = 1L;
	//should be same over the whole mention group
	//although in some strategies, the id-pair could be synthetic (multiple different term pairs grouped together because the share types and relation set)
	public String id1;
	public String id2;
	public String type1;
	public String type2;
	public Collection<String> relTypes;
	
	public Span span1;
	public Span span2;
	public String sentence;
	
	public String documentId;
	
	protected void setFields(String[] tsvparts) {
        if (tsvparts.length != 8 && tsvparts.length != 9)
            throw new IllegalArgumentException("Bad tsv line "+tsvparts.length+": "+Lang.stringList(tsvparts, "\t"));
        this.id1 = tsvparts[0];
        this.id2 = tsvparts[1];
        this.type1 = tsvparts[2];
        this.type2 = tsvparts[3];
        this.span1 = Span.fromString(tsvparts[4]);
        this.span2 = Span.fromString(tsvparts[5]);
        this.relTypes = tsvparts[6].isEmpty() ? Collections.EMPTY_LIST : Arrays.asList(tsvparts[6].split(","));
        this.sentence = tsvparts[7];
        if (tsvparts.length > 8)
            this.documentId = tsvparts[8];
	}
	
	public RelexMention() {};
	
	public void fromString(String tsvLine) {
		String[] tsvparts = tsvLine.split("\t");
		setFields(tsvparts);
	}
	
	public RelexMention(
			String id1, String id2,
			String type1, String type2,
			Span span1, Span span2,
			Collection<String> relTypes, String sentence,
			String documentId) 
	{
		this.id1 = id1;
		this.id2 = id2;
		this.type1 = type1;
		this.type2 = type2;
		this.span1 = span1;
		this.span2 = span2;
		this.relTypes = relTypes;
		this.sentence = sentence;
		this.documentId = documentId;
	}
	
	@Override
	public String groupId() {
		return id1 + "\t" + id2;
	}
	
	public int groupSplit(int splitCount) {
		int splitNdx = (int)Math.floor(RandomUtil.pseudoRandomFromString("GS:"+this.sentence)*splitCount);
		if (splitNdx >= splitCount)
		    splitNdx = splitCount - 1;
		return splitNdx;
	}
	
	@Override
	public String entitySetId() {
		return GroundTruth.getOrderedIdPair(id1,id2);
	}
	
	@Override
	public boolean isNegative() {
		return this.relTypes.isEmpty();
	}
	
	@Override
	public double getNegativeDownsamplePriority() {
		return GroundTruth.getDownsamplePriority(id1, id2);
	}
	
	@Override
	public double getDatasetSplitPosition() {
		return GroundTruth.getSplitLocation(id1, id2);
	}
	
	@Override
	public String[] getTypes() {
		return new String[] {type1, type2};
	}
	
	@Override
	public String[] getRelations() {
		return relTypes.toArray(new String[relTypes.size()]);
	}
	
	@Override
	public String[] getTokens(Annotator tokenizer) {
		Document d = new Document(sentence);
		tokenizer.process(d);
		String[] toks = d.toStringArray(d.getAnnotations(Token.class));
		return toks;
	}
	
	public String uniquenessString() {
		return span1.toString()+"\t"+span2.toString()+"\t"+sentence;
	}
	

	public static <M extends IRelexMention> List<M> addNoDuplicates(List<M> ms1, List<M> ms2) {
		if (ms1.isEmpty())
			return ms2;
		if (ms2.isEmpty())
			return ms1;
		List<M> ms = null;
		List<M> madds = null;
		if (ms1.size() >= ms2.size()) {
			ms = ms1;
			madds = ms2;
		} else {
			ms = ms2;
			madds = ms1;
		}

		if (ms.size() < 40) {
			for (M madd : madds) {
				String maddId  = madd.uniquenessString();
				boolean found = false;
				for (M m : ms) {
					String id = m.uniquenessString();
					if (maddId.equals(id)) {
						found = true;
						break;
					}
				}
				if (!found)
					ms.add(madd);
			}
		} else {
			Set<String> ids = new HashSet<>();
			for (M m : ms) {
				ids.add(m.uniquenessString());
			}
			for (M madd : madds) {
				if (!ids.contains(madd.uniquenessString())) {
					ms.add(madd);
				}
			}
		}
		return ms;
	}
	
	protected void tsvCheck(String str) {
		if (str.indexOf('\t') != -1 || str.indexOf('\n') != -1)
			throw new IllegalStateException("illegal whitespace in "+str);
	}
	
	public void validate() {
		if (!span1.isValid(sentence) || !span2.isValid(sentence) || span1.overlaps(span2)) {
			throw new IllegalStateException("bad spans "+span1+", "+span2+" for "+sentence.length());
		}
		tsvCheck(id1);
		tsvCheck(id2);
		tsvCheck(type1);
		tsvCheck(type2);
		for (String relType : relTypes) {
			tsvCheck(relType);
			if (relType.indexOf(',') != -1)
				throw new IllegalStateException("relation types cannot have comma: "+relType);
		}
	}
	
	/**
	 * aligns with fromString
	 */
	public String toString() {
		return id1+"\t"+id2+"\t"+
			type1+"\t"+type2+"\t"+
			Span.toString(span1)+"\t"+Span.toString(span2)+"\t"+
			Lang.stringList(relTypes,  ",")+"\t"+
			sentence.replace('\t', ' ').replace('\n', ' ')+
			(documentId != null ? "\t"+documentId : "");
	}
	
	/**
	 * Checks that the set is non-empty and contains mentions in the same group
	 * @param mentions
	 * @return
	 */
	public static boolean isValidSet(Iterable<? extends IRelexMention> mentions) {
		String m1group = null;
		for (IRelexMention m : mentions) {
			if (m1group == null)
				m1group = m.groupId();
			if (!m.groupId().equals(m1group))
				return false;
		}
		if (m1group == null)
			return false;
		return true;
	}
	
	public static class Writer implements AutoCloseable {
		protected PrintStream out;
		
		public Writer(File tsvFile) {
			out = FileUtil.getFilePrintStream(tsvFile.getAbsolutePath());
		}
		
		public synchronized void write(RelexMention m) {
			m.validate();
			out.println(m.toString());
		}
				
		@Override
		public void close() {
			if (out != null) {
				out.close();
				out = null;
			}
		}
		
	}

    @Override
    public String toSupportString() {
        String escapeSent = sentence.replace('\n', ' ').replace('\t', ' ').replace("<<<", "   ").replace(">>>", "   ");
        return Span.highlightAll(escapeSent, Arrays.asList(span1, span2), "<<<", ">>>");
    }

    @Override
    public double getDocumentLearningCurvePosition() {
        if (documentId == null)
            throw new UnsupportedOperationException();
        return new Random(documentId.hashCode() + 123321).nextDouble();
    }
}