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

import com.google.common.base.*;
import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.*;


/**
 * represents offset annotations on a document of text
 * 'document' is a very general notion here, it could be a keyword query for example
 * 
 * @author mrglass
 *
 */
public class Document implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public final String id;
	public String text;
	//CONSIDER: also support a 'subtype' index?
	protected Map<Class,ArrayList<Annotation>> annotations = new HashMap<>();
	protected Map<Class,DocumentStructure> docLevelStructures = new HashMap<>();
	
	private static final boolean DEBUG_MODE = false;
	
	//CONSIDER: make Annotation implement DocumentStructure
	//  then change AnnoRef to hold reference to DocumentStructure
	//  then allow a replaceDocumentStructure method
	//  the other reason to make Annotation implement DocumentStructure is to simplify the ReadableSerialization
	
	//we use these for Annotations that hold a reference to other annotations: Token, CorefIndex.Chain, Relation, and others
	@SuppressWarnings("rawtypes")
    protected Map<Annotation,AnnoRef> annoRefs;
	@SuppressWarnings("unchecked")
    public <T extends Annotation> AnnoRef<T> getAnnoRef(T a) {
	    if (a == null)
	        throw new IllegalArgumentException();
		if (annoRefs == null)
			annoRefs = new HashMap<>();
		AnnoRef<T> r = annoRefs.get(a);
		if (r == null) {
			r = new AnnoRef<T>(a);
			annoRefs.put(a,r);
		}
		return r;
	}
	public void replaceAnnotation(Annotation old, Annotation repl) {
		if (annoRefs != null) {
			AnnoRef r = annoRefs.remove(old);
			if (r != null) {
				r.set(repl);
				annoRefs.put(repl, r);
			}
		}
		this.removeAnnotation(old);
		this.addAnnotation(repl);
	}
	
	/**
	 * create document with randomUUID
	 * @param text
	 */
	public Document(String text) {
		this.id = UUID.randomUUID().toString();
		this.text = text;
	}
	
	public Document(String id, String text) {
		this.id = id;
		this.text = text;
	}
	
	/**
	 * Changes the text of the document, and adjust the spans to match the text before the document was transformed
	 * @param origText
	 * @param correct
	 */
	public void untransform(String origText, OffsetCorrection correct) {
		this.text = origText;
		for (Annotation anno : getAnnotations(Annotation.class)) {
			correct.correct(anno);
		}
	}
	
	public String[] toStringArray(Collection<? extends Annotation> anno) {
		String[] ar = new String[anno.size()];
		int ndx = 0;
		for (Annotation a : anno)
			ar[ndx++] = a.substring(text);
		return ar;
	}
	
	public String coveredText(Annotation a) {
		return a.coveredText(this);
	}
	
	public String coveredText(List<? extends Span> spanList) {
		return this.text.substring(spanList.get(0).start, spanList.get(spanList.size()-1).end);
	}
	
	public List<String> toStrings(Iterable<? extends Annotation> anno) {
		ArrayList<String> strs = new ArrayList<String>();
		for (Annotation a : anno) {
			strs.add(a.substring(text));
		}
		return strs;
	}
	
	public <T extends DocumentStructure> T getDocumentStructure(final Class<T> type) {
		if (docLevelStructures == null)
			docLevelStructures = new HashMap<>();
		return (T)docLevelStructures.get(type);
	}
	
	/**
	 * Note document structures are only retrievable by their specific type.
	 * @param docStruct
	 */
	public void setDocumentStructure(DocumentStructure docStruct) {
		Class dc = docStruct.getClass();
		//do {
			docLevelStructures.put(dc, docStruct);
		//} while ((dc = dc.getSuperclass()) != null);
	}
	
	public Collection<Class> getDocumentStructuresPresent() {
		return Lang.NVL(docLevelStructures, Collections.EMPTY_MAP).keySet();
	}
	
	//CONSIDER: sorting shorter Annotations first would let us do this with two binary searches
	@Deprecated
	public <T extends Annotation> List<T> getAnnotationsList(final Class<T> type, final Span within) {
		return getAnnotations(type, within);
	}
	
	public <T extends Annotation> List<T> getAnnotations(final Class<T> type, final Span within) {	
		List<T> annoList = getAnnotationsInternal(type);
		if (annoList == null)
			return Collections.EMPTY_LIST;
		List<T> annos = new ArrayList<>();
		int startPos = Collections.binarySearch(annoList, within);
		if (startPos < 0)
			startPos = -startPos - 1;
		for (int i = startPos; i < annoList.size() && annoList.get(i).start <= within.end; ++i) {
			if (annoList.get(i).end <= within.end)
				annos.add(annoList.get(i));
		}
		if (DEBUG_MODE) {
			List<T> annosCheck = new ArrayList<>();
			Iterables.addAll(annosCheck, Iterables.filter(getAnnotations(type), new Predicate<Annotation>() {
				@Override
				public boolean apply(Annotation arg0) {
					return within.contains(arg0);
				}
			}));
			if (!annosCheck.equals(annos)) {
				System.err.println("requested "+within);
				System.err.println("got         "+Lang.stringList(annos, ";; "));
				System.err.println("rather than "+Lang.stringList(annosCheck, ";; "));
				throw new Error("Bad implementation.");
			}
		}
		//CONSIDER: this could be even better if returning a sublist when the annotations within the span are continuous
		//  it will happen every time we select all tokens within a sentence for example
		
		return annos;
		
		//Old method
		/*
		return Iterables.filter(getAnnotations(type), new Predicate<Annotation>() {
			@Override
			public boolean apply(Annotation arg0) {
				return within.contains(arg0);
			}
		});
		*/
	}
	
	/**
	 * removes all annotations and document level structures
	 */
	public void clear() {
		annotations.clear();
		docLevelStructures.clear();
	}
	
	public boolean removeAnnotation(Annotation anno) {
		return removeAnnotation(anno, null);
	}
	
	protected boolean removeAnnotation(Annotation anno, ArrayList<? extends Annotation> notThisOne) {
		if (annoRefs != null)
			annoRefs.remove(anno);
		Class ac = anno.getClass();
		Class end = Annotation.class.getSuperclass();
		boolean found = false;
		do {
			ArrayList<Annotation> annoList = annotations.get(ac);
			if (annoList != null && annoList != notThisOne) {
				found |= annoList.remove(anno);
			}
			//CONSIDER: if (annoList.isEmpty()) annotations.remove(ac);
		} while (end != (ac = ac.getSuperclass()));
		return found;
	}
	
	public <T extends Annotation> boolean removeAnnotations(final Class<T> type) {
		boolean found = false;
		ArrayList<T> annoList = (ArrayList<T>)annotations.get(type);
		if (annoList == null)
			return false;
		Iterator<T> ait = annoList.iterator();
		while (ait.hasNext()) {
			T a = ait.next();
			found = true;
			ait.remove();
			//CONSIDER: pretty inefficient, we could put the supertype climbing in here
			removeAnnotation(a, annoList);
		}
		cleanIndex();
		
		return found;		
	}

	protected void cleanIndex() {
		Collection<Class> empty = new ArrayList<>();
		for (Map.Entry<Class, ArrayList<Annotation>> e : annotations.entrySet()) {
			if (e.getValue().isEmpty())
				empty.add(e.getKey());
		}
		HashMapUtil.removeAll(annotations, empty);
	}
	
	/**
	 * remove all annotations that are instanceof type and satisfying shouldRemove
	 * @param type
	 * @param shouldRemove
	 * @return true iff any annotations were removed
	 */
	public <T extends Annotation> boolean removeAnnotations(final Class<T> type, Predicate<T> shouldRemove) {
		boolean found = false;
		ArrayList<T> annoList = (ArrayList<T>)annotations.get(type);
		if (annoList == null)
			return false;
		Iterator<T> ait = annoList.iterator();
		while (ait.hasNext()) {
			T a = ait.next();
			if (shouldRemove.apply(a)) {
				found = true;
				ait.remove();
				removeAnnotation(a, annoList);
			}
		}
		cleanIndex();
		return found;
	}
	
	/**
	 * remove all annotations satisfying shouldRemove
	 * @param shouldRemove
	 * @return true iff any annotations were removed
	 */
	public boolean removeAnnotations(Predicate<Annotation> shouldRemove) {
		return removeAnnotations(Annotation.class, shouldRemove);
	}
	
	protected <T extends Annotation> List<T> getAnnotationsInternal(final Class<T> type) {
		return (List<T>)annotations.get(type);
	}
	
	/**
	 * true if the Document has any annotation that is instanceof 'type'
	 * @param type
	 * @return
	 */
	public <T extends Annotation> boolean hasAnnotations(final Class<T> type) {
		List<T> annos = getAnnotationsInternal(type);
		return (annos != null && !annos.isEmpty());
	}
	
	/**
	 * Get the annotations of the specified type.
	 * Retrieves all annotations added that are instanceof the supplied type
	 * @param type
	 * @return never null, empty if not present
	 */
	public <T extends Annotation> List<T> getAnnotations(final Class<T> type) {
		return Collections.unmodifiableList(Lang.NVL(getAnnotationsInternal(type), (List<T>)Collections.EMPTY_LIST));
	}
	
	/**
	 * gets the single annotation of type 'type'
	 * null if not present
	 * IllegalArgumentException if multiple present
	 * @param type
	 * @return
	 */
	public <T extends Annotation> T getSingleAnnotation(final Class<T> type) {
		Collection<T> annos = getAnnotationsInternal(type);
		if (annos == null || annos.isEmpty())
			return null;
		if (annos.size() > 1)
			throw new IllegalArgumentException("Annotation type "+type+" present "+annos.size()+" times");
		return annos.iterator().next();
	}
	
	public <T extends Annotation> T getSingleAnnotation(final Class<T> type, Span within) {
		Collection<T> annos = getAnnotations(type, within);
		if (annos == null || annos.isEmpty())
			return null;
		if (annos.size() > 1)
			throw new IllegalArgumentException("Annotation type "+type+" present "+annos.size()+" times");
		return annos.iterator().next();
	}
	
	/**
	 * false if the annotation is already present (duplicates are ignored);
	 * true if it was added
	 * @param anno
	 * @return
	 */
	public boolean addAnnotation(Annotation anno) {
		//we allow this since the Annotation's span could be corrected later, or the text updated
		//but annotation spans really should not change - since this may result in the annotation lists being out of order
		//if (!anno.isValid(this.text))
		//	throw new IllegalArgumentException("Annotation out of bounds: "+anno.start+", "+anno.end+" but text is "+text.length()+" chars long");
		
		//check if any superclasses are also Annotation, add to these as well
		Class ac = anno.getClass();
		Class end = Annotation.class.getSuperclass();
		do {
			ArrayList<Annotation> annoList = annotations.get(ac);
			if (annoList == null) {
				annoList = new ArrayList<>();
				annotations.put(ac, annoList);
			}
			if (annoList.isEmpty() || annoList.get(annoList.size()-1).compareTo(anno) <= 0) {
				if (!annoList.isEmpty() && annoList.get(annoList.size()-1) == anno)
					return false;
				annoList.add(anno);
			} else {
				int pos = Collections.binarySearch(annoList, anno);
			    if (pos < 0) {
			        annoList.add(-pos-1, anno);
			    } else {
			    	//check if we tried to re-add exact same annotation
			    	if (alreadyPresentAnnotation(annoList, pos, anno))
			    		return false;
			    	
			    	annoList.add(pos, anno);
			    }
			}
		} while (end != (ac = ac.getSuperclass()));
		return true;
	}
	
	private boolean alreadyPresentAnnotation(List<Annotation> annoList, int equalSpanNdx, Annotation anno) {
    	//get to first of equal span annotations
    	while (equalSpanNdx-1 >= 0 && annoList.get(equalSpanNdx-1).matchingSpan(anno))
    		--equalSpanNdx;
    	
    	//check all equal span annotations to see if they are ==
    	while (equalSpanNdx < annoList.size() && annoList.get(equalSpanNdx).matchingSpan(anno)) {
    		if (annoList.get(equalSpanNdx) == anno) 
    			return true;
    		++equalSpanNdx;
    	}
    	
    	return false;
	}
	
	/**
	 * checks if each segmentation is present, will return first in argument list, or a single annotation covering the entire document
	 * typical use is: doc.getSegmentation(Sentence.class, Paragraph.class)
	 * @param bestToWorst
	 * @return
	 */
	public List<Annotation> getSegmentation(Class... bestToWorst) {
		for (Class segCls : bestToWorst) {
			if (this.hasAnnotations(segCls)) {
				return this.getAnnotations(segCls);
			}
		}
		return Collections.singletonList(new Annotation("dummydoc", 0, this.text.length()));
	}
	
	/**
	 * Decompose this document into separate documents.
	 * The annotations on this document will be (re)moved to the sub-documents, leaving this document clear.
	 * Any annotations crossing the segment boundaries will be dropped.
	 * All sub documents will have the same id.
	 * No document level structures will be in the sub-documents.
	 * To keep original Document intact call Lang.deepCopy(doc).decomposeIntoSubDocuments.
	 * @param segments
	 * @return
	 */
	public List<Document> decomposeIntoSubDocuments(Iterable<? extends Span> segments) {
		if (Span.overlap(segments))
			throw new IllegalArgumentException("segmentation must not overlap");
		List<Document> subs = new ArrayList<>();
		List<Integer> offsets = new ArrayList<>();
		for (Span span : segments) {
			Document sub = new Document(this.id, span.substring(this.text));
			for (Annotation a : this.getAnnotations(Annotation.class)) {
				if (span.contains(a)) {
					sub.addAnnotation(a);
				}
			}
			subs.add(sub);
			offsets.add(span.start);
		}
		//correct offsets
		for (int i = 0; i < subs.size(); ++i) {
			int offset = offsets.get(i);
			for (Annotation a : subs.get(i).getAnnotations(Annotation.class)) {
				a.addOffset(-offset);
			}
		}
		return subs;
	}
	
	//just used when an AnnotatorService returns the modified Document as a copy
	public void copyModifiedFrom(Document doc) {
		//normally text should not change; 
		//but id should definitely not change
		this.text = doc.text;
		this.annotations = doc.annotations;
		this.docLevelStructures = doc.docLevelStructures;
	}
	
	/**
	 * Consider toSimpleInlineMarkupInstead(), it will show overlapping annotations
	 * @param annos
	 * @return
	 */
	public String highlight(Iterable<? extends Annotation> annos) {
		return Annotation.highlightAll(text, annos);
	}
	/**
	 * Consider toSimpleInlineMarkupInstead(), it will show overlapping annotations
	 * @param type
	 * @return
	 */
	public String highlight(Class<? extends Annotation> type) {
		return highlight(this.getAnnotations(type));
	}
	
	
	private static final class InlineTag implements Comparable<InlineTag> {
		int docCharNdx;
		boolean isStart;
		String tagType;
		int annoNum;
		
		int length;
		
		public static List<InlineTag> makeInlineTags(Iterable<? extends Annotation> annos) {
			List<InlineTag> itags = new ArrayList<>();
			int annoNum = 0;
			for (Annotation a : annos) {
				InlineTag s = new InlineTag();
				InlineTag e = new InlineTag();
				s.docCharNdx = a.start;
				e.docCharNdx = a.end;
				s.length = a.length();
				e.length = a.length();
				s.tagType = a.highlightLabel();
				e.tagType = s.tagType;
				if (e.tagType.indexOf(':') != -1)
				    e.tagType = e.tagType.substring(0, e.tagType.indexOf(':'));
				s.isStart = true;
				s.annoNum = annoNum;
				e.annoNum = annoNum;
				++annoNum;
				itags.add(s);
				itags.add(e);
			}
			Collections.sort(itags);
			return itags;
		}
		
		public String toString() {
			return (isStart ? "<" : "</") + tagType + ":" + annoNum + ">";
		}
		
		@Override
		public int compareTo(InlineTag o) {
			if (docCharNdx != o.docCharNdx)
				return docCharNdx - o.docCharNdx;
			if (annoNum == o.annoNum) //zero length annotations
				return isStart ? -1 : 1;
			if (isStart != o.isStart)
				return isStart ? 1 : -1;
			if (length != o.length)
				return (isStart ? -1 : 1) * (length - o.length);
			if (annoNum == o.annoNum)
				throw new IllegalStateException();
			return (isStart ? 1 : -1) * (annoNum - o.annoNum);
		}	
	}
	
	/**
	 * No attempt is made to escape the text, so this result is not reliably parseable back into Document format
	 * Just for display
	 * also adds ids to the tags in a way that is not valid xml
	 * @return
	 */
	public String toSimpleInlineMarkup() {
		return toSimpleInlineMarkup(new Span(0,text.length()), this.getAnnotations(Annotation.class));
	}
	
	public String toSimpleInlineMarkup(Span segment, Iterable<? extends Annotation> annos) {
		segment.fix(0, text.length());
		List<InlineTag> itags = InlineTag.makeInlineTags(annos);
		int tagNdx = 0;
		StringBuilder buf = new StringBuilder();
		//move to first tag in segment
		while (tagNdx < itags.size() && itags.get(tagNdx).docCharNdx < segment.start)
			++tagNdx;
		for (int i = segment.start; i <= segment.end; ++i) {
			while (tagNdx < itags.size() && i == itags.get(tagNdx).docCharNdx) {
				buf.append(itags.get(tagNdx).toString());
				++tagNdx;
			}
			
			if (i < segment.end)
				buf.append(text.charAt(i)); //CONSIDER: flag to enable xml escape? StringEscapeUtils.escapeXml11(input)
		}
		return buf.toString();
	}
	
	public String toSimpleInlineMarkup(Span segment) {
		return toSimpleInlineMarkup(segment, getAnnotations(Annotation.class, segment));
	}
	
	public String toSimpleInlineMarkup(Span segment, Function<Annotation,Boolean> toInclude) {
		segment.fix(0, text.length());
		List<Annotation> annos = new ArrayList<>();
		for (Annotation anno : this.getAnnotations(Annotation.class, segment))
			if (toInclude.apply(anno))
				annos.add(anno);
		return toSimpleInlineMarkup(segment, annos);
	}
	
	//TODO: better serialization
	//Kryo
	//improve JSON
	
	//TODO: validation methods
	/*  
    multi-threading gives same results as single threading
    detect what annotations it depends on
    verify annotations always in sorted order
    check what annotations are modified
    check clearNLP segmentation
    check Overlapping - compare to linear scan
    check no duplicate annotations
    check annotations are maintained in sorted order
    check that annotation is retrievable from all superclasses
    document reader / writer order maintained - documents are the same
    reversible transformations
	 */
}
