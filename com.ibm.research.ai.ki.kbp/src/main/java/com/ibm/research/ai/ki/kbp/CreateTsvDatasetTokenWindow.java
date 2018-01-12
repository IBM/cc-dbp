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


import java.util.*;

import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.RelexConfig.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Document Collection + GroundTruth -> tsv dataset.
 * Documents must have EntityWithId identified and be tokenized.
 * If Title and SectionHeader annotations are present then we add those to the sentence.
 * 
 * @author mrglass
 *
 */
public class CreateTsvDatasetTokenWindow implements IRelexTsv<RelexMention> {
	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 * @param gt the ground truth, null for test/apply
	 * @param negativeExampleSampleFraction 0.05 to take 5% of negatives
	 */
	public CreateTsvDatasetTokenWindow(GroundTruth gt, RelexConfig config) {
		this.gt = gt;
		this.config = config;
		if (gt == null && config.negativeExampleSampleFraction != 1.0) {
			throw new IllegalArgumentException("Can't downsample negatives without ground truth!");
		}
		if (config.entityPairFilterClass != null) {
			try {
				filter = (IEntityPairFilter)Class.forName(config.entityPairFilterClass).newInstance();
				filter.initialize(gt, config);
			} catch (Exception e) {
				throw new Error(e);
			}
		} else {
			filter = null;
		}
		idsOfInterest = config.limitEntitiesToGroundTruth ? gt.getRelevantIds() : null;
		
		preprocess = new DocumentPreprocessing().getPipeline(config, gt);
	}
	
	protected GroundTruth gt;
	public final RelexConfig config;
	public final IEntityPairFilter filter;
	protected Set<String> idsOfInterest;
	protected Pipeline preprocess;
	private boolean isPreprocessInitialized = false;
	
	protected Iterable<SectionHeader> getSectionHeaders(Document doc, Span s) {
		return Iterables.filter(doc.getAnnotations(SectionHeader.class), 
				sh -> sh.sectionBody != null && sh.sectionBody.get().contains(s));
	}
	
	private static final String sectionTitleSep = " ===== ";
	
	private static final String docFeatureSep = " ***** ";
	
	protected boolean isValid(EntityWithId e) {
	    return (e.id != null && (idsOfInterest == null || idsOfInterest.contains(e.id)));
	}
	
	//to work on spark, as well as single threaded - the core function should map Document to List<RelexMention>
	//  where the RelexMentions are not (necessarily) grouped
	//  similar to ExplorePathGen.getInstances(gt, FileUtil.objectFromBase64String(base64enc));
	//  except we don't do the feature gen in this phase
	//doc must be sentence segmented, and have EntityWithId identified.
	//if Title and SectionHeader annotations are present then we add those to the sentence
	public List<RelexMention> getMentions(Document doc) {
		if (doc == null)
			return Collections.EMPTY_LIST;
		synchronized (preprocess) {
			if (!isPreprocessInitialized) {
				preprocess.initialize(new Properties());
				isPreprocessInitialized = true;
			}
		}
		preprocess.process(doc);
		List<RelexMention> mentions = new ArrayList<>();
		Title title = doc.getSingleAnnotation(Title.class);

		CombinedSpans toSkip = new CombinedSpans();
		//we don't view title and section alone as a source for relation mentions
		if (config.titleContext && title != null)
			toSkip.add(new Span(title));
		if (config.sectionContext) {
    		for (SectionHeader h : doc.getAnnotations(SectionHeader.class)) {
    			toSkip.add(new Span(h));
    		}
		}
		
		List<EntityWithId> ents = doc.getAnnotations(EntityWithId.class);
		List<Token> tokens = doc.getAnnotations(Token.class);
		List<Span> entToks = Span.toSegmentSpans(ents, tokens);
		for (int ei = 0; ei < entToks.size(); ++ei) {
			if (entToks.get(ei) == null) {
				throw new IllegalStateException("No tokens for entity: "+doc.toSimpleInlineMarkup(
	    			new Span(ents.get(ei).start - 50, ents.get(ei).end+50), 
	    			a -> (a instanceof EntityWithId) || (a instanceof Token)));
			}
			
	    }
		
		//TODO: migrate this logic into the other two tsv dataset generators
		String docFeaturesString = null;
		DocumentFeatureString dfs = doc.getDocumentStructure(DocumentFeatureString.class);
		if (dfs != null) {
		    docFeaturesString = dfs.featureString;
		}
		
		//for each entity
		for (int ei = 0; ei < ents.size(); ++ei) {
		    //now find entities that are within the token window
		    EntityWithId ecenter = ents.get(ei);
		    if (!isValid(ecenter) || toSkip.contains(ecenter))
		        continue;		    
		    
		    StringBuilder stbuf = new StringBuilder();
		    List<Pair<EntityWithId, Integer>> entityAndOffset = new ArrayList<>();
		    if (docFeaturesString != null) {
		        stbuf.append(docFeaturesString).append(docFeatureSep);
		    }
		    //entities in the sections and headers
		    if (config.sectionContext) {
    		    for (SectionHeader h : getSectionHeaders(doc, ecenter)) {
    		        for (EntityWithId e : doc.getAnnotations(EntityWithId.class, h)) {
                        if (isValid(e))
                            entityAndOffset.add(Pair.of(e, stbuf.length()-h.start));
                    }
                    stbuf.append(h.coveredText(doc)).append(sectionTitleSep);
    		    }
		    }
		    if (config.titleContext && title != null) {
		        for (EntityWithId e : doc.getAnnotations(EntityWithId.class, title)) {
	                if (isValid(e))
	                    entityAndOffset.add(Pair.of(e, stbuf.length()-title.start));
	            }
	            stbuf.append(title.coveredText(doc)).append(sectionTitleSep);
		    }
		    
		    int tokStartNdx = Math.max(0, entToks.get(ei).start - config.mentionTokenWindowSize);
            int tokEndNdx = Math.min(tokens.size(), entToks.get(ei).end + config.mentionTokenWindowSize) - 1;
            int windowStart = tokens.get(tokStartNdx).start;
            int windowEnd = tokens.get(tokEndNdx).end;
            
            Pair<EntityWithId, Integer> centerEntity = Pair.of(ecenter, stbuf.length() - windowStart);
		    for (int oei = ei+1; oei < ents.size() && entToks.get(oei).end <= tokEndNdx+1; ++oei) {
                //these ents are within the window and after
		        EntityWithId e = ents.get(oei);
		        if (isValid(e) && !toSkip.contains(e))
                    entityAndOffset.add(Pair.of(e, stbuf.length() - windowStart));
            }
		    
		    stbuf.append(doc.text.substring(windowStart, windowEnd));
		    String sentence = stbuf.toString();
		    
		    for (Pair<EntityWithId, Integer> oe : entityAndOffset) {
		        String id1 = centerEntity.first.id;
		        String id2 = oe.first.id;
		        String type1 = centerEntity.first.type;
                String type2 = oe.first.type;
		        if (filter != null && !filter.test(id1, type1, id2, type2))
                    continue;
		        Collection<String> reltypes = gt != null ? gt.getRelations(id1, id2) : Collections.EMPTY_LIST;
                if (reltypes.isEmpty() && 
                        config.negativeExampleSampleFraction < 1.0 && 
                    GroundTruth.getDownsamplePriority(id1, id2) > config.negativeExampleSampleFraction) 
                {
                    continue;
                }
		        addMentions(mentions, centerEntity, oe, sentence, doc.id, reltypes);
		    }
		}
		
		return mentions;
	}
	
	protected String getDirection(Collection<String> reltypes) {
		String dir = null;
		for (String rt : reltypes) {
			String diri = rt.substring(0,1);
			if (dir == null)
				dir = diri;
			else if (!dir.equals(diri))
				throw new UnsupportedOperationException("clashing relation directions: "+Lang.stringList(reltypes, ", "));
		}
		return dir;
	}
	
	protected Collection<String> reverseRels(Collection<String> reltypes) {
		List<String> rev = new ArrayList<>();
		for (String rt : reltypes) {
			String dir = rt.substring(0,1);
			String rel = rt.substring(1);
			rev.add((dir.equals(">") ? "<" : ">") + rel);
		}
		return rev;
	}
	
	protected Collection<String> ignoreDirection(Collection<String> reltypes) {
		Set<String> ig = new HashSet<>();
		for (String rt : reltypes) {
			String rel = rt.substring(1);
			ig.add(rel);
		}
		return ig;
	}
	
	protected void addMentions(List<RelexMention> mentions, 
			Pair<EntityWithId,Integer> e1m, Pair<EntityWithId,Integer> e2m, 
			String sentence, String docId, Collection<String> rawRelTypes) 
	{
		if (config.directionStyle == DirectionStyle.ignore) {
			mentions.add(makeMention(e1m, e2m, sentence, docId, ignoreDirection(rawRelTypes)));
		} else if (config.directionStyle == DirectionStyle.bothWays) {
			mentions.add(makeMention(e1m, e2m, sentence, docId, rawRelTypes));
			mentions.add(makeMention(e2m, e1m, sentence, docId, reverseRels(rawRelTypes)));
		} else if (config.directionStyle == DirectionStyle.fixed) {
			String direction = getDirection(rawRelTypes);
			if (direction == null) {
				//negative
				mentions.add(makeMention(e1m, e2m, sentence, docId, Collections.EMPTY_LIST));
				mentions.add(makeMention(e2m, e1m, sentence, docId, Collections.EMPTY_LIST));
				return;
			}
			
			boolean negTheOtherWay = false;
			if (config.negativeExampleSampleFraction >= 1.0 || 
				GroundTruth.getDownsamplePriority(e1m.first.id, e2m.first.id) < config.negativeExampleSampleFraction) 
			{
				//and with negative downsample prob add it the other way
				negTheOtherWay = true;
			}
			
			if (">".equals(direction)) {
				mentions.add(makeMention(e1m, e2m, sentence, docId, ignoreDirection(rawRelTypes)));
				if (negTheOtherWay)
					mentions.add(makeMention(e2m, e1m, sentence, docId, Collections.EMPTY_LIST));
			} else if ("<".equals(direction)) {
				mentions.add(makeMention(e2m, e1m, sentence, docId, ignoreDirection(rawRelTypes)));
				if (negTheOtherWay)
					mentions.add(makeMention(e1m, e2m, sentence, docId, Collections.EMPTY_LIST));
			} else {
				throw new UnsupportedOperationException(direction);
			}
		} else {
			throw new UnsupportedOperationException("unknown direction style = "+config.directionStyle);
		}
	}
	
	protected RelexMention makeMention(Pair<EntityWithId,Integer> e1m, Pair<EntityWithId,Integer> e2m, String sentence, String docId, Collection<String> reltypes) {
		RelexMention m = new RelexMention();
		m.id1 = e1m.first.id;
		m.id2 = e2m.first.id;
		m.type1 = config.gtTypes ? Lang.NVL(gt.getType(m.id1), "unk") : e1m.first.type;
		m.type2 = config.gtTypes ? Lang.NVL(gt.getType(m.id2), "unk") : e2m.first.type;
		m.span1 = new Span(e1m.first);
		m.span1.addOffset(e1m.second);
		m.span2 = new Span(e2m.first);
		m.span2.addOffset(e2m.second);
		m.sentence = sentence;
		m.relTypes = reltypes;
		m.documentId = docId.replace('\t', ' ').replace('\n', ' ');
		return m;
	}
		

}
