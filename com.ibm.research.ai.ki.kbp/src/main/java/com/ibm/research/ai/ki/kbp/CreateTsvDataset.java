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
import java.util.concurrent.atomic.*;

import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.RelexConfig.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.parallel.*;

/**
 * Document Collection + GroundTruth -> tsv dataset.
 * Documents must be sentence segmented, and have EntityWithId identified.
 * If Title and SectionHeader annotations are present then we add those to the sentence.
 * 
 * @author mrglass
 *
 */
public class CreateTsvDataset implements IRelexTsv<RelexMention> {
	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 * @param gt the ground truth, null for test/apply
	 * @param negativeExampleSampleFraction 0.05 to take 5% of negatives
	 */
	public CreateTsvDataset(GroundTruth gt, RelexConfig config) {
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
	
	protected Iterable<SectionHeader> getSectionHeaders(Document doc, Sentence sent) {
		return Iterables.filter(doc.getAnnotations(SectionHeader.class), 
				sh -> sh.sectionBody != null && sh.sectionBody.get().contains(sent));
	}
	
	//closest without overlapping
	protected Pair<Integer,Integer> bestSpanPair(List<EntityWithId> id1, List<EntityWithId> id2) {
		double bestScore = Double.NEGATIVE_INFINITY;
		Pair<Integer,Integer> bestPair = new Pair<>(null,null);
		for (int i1 = 0; i1 < id1.size(); ++i1) {
			EntityWithId c1 = id1.get(i1);
			for (int i2 = 0; i2 < id2.size(); ++i2) {
				EntityWithId c2 = id2.get(i2);
				double score = c1.overlaps(c2) ? Double.NEGATIVE_INFINITY : -c1.distanceTo(c2);
				if (score > bestScore) {
					bestScore = score;
					bestPair.first = i1;
					bestPair.second = i2;
				}
			}
		}
		
		return bestPair.first != null ? bestPair : null;
	}
	
	private static final String sectionTitleSep = " ===== ";
	
	//single threaded version of reduceByKey and RelexMention.Writer
	private void write(Iterable<RelexMention> allMentions, File tsvFile) {	
		Map<String,ArrayList<RelexMention>> gid2mentions = new HashMap<>();
		for (RelexMention m : allMentions) {
			HashMapUtil.addAL(gid2mentions, m.id1+"\t"+m.id2, m);
		}
		//report coverage of the triple set (how many triples with at least one mention, how many with two, etc)
		if (gt != null) {
			System.out.println("Triple set coverage / Mentions per relation instance:");
			System.out.println(gt.reportMentions(idp -> Lang.NVL(gid2mentions.get(idp), Collections.EMPTY_LIST).size()));
		}
		try (RelexMention.Writer writer = new RelexMention.Writer(tsvFile)) {
			for (List<RelexMention> ms : gid2mentions.values()) {
				for (RelexMention m : ms)
					writer.write(m);
			}
		}
	}
	
	/**
	 * Construct the text of the sentence and find the spans for entities in it.
	 * @param doc
	 * @param title
	 * @param sent
	 * @param idsOfInterest
	 * @param idsPresent
	 * @return
	 */
	protected String getSentenceAndFillIds(
			Document doc, Title title, Sentence sent, 
			Set<String> idsOfInterest, Map<String,ArrayList<Pair<EntityWithId,Integer>>> idsPresent) 
	{
		int tokenCount = 0;
		//if there are multiple mentions with the same id in the sentence, maybe just choose one per other id
		idsPresent.clear();
		//find ids in the sentence, section headers and title
		StringBuilder stbuf = new StringBuilder();
		if (config.sectionContext) {
			for (SectionHeader h : getSectionHeaders(doc, sent)) {
				for (EntityWithId e : doc.getAnnotations(EntityWithId.class, h)) {
				    if (e.id != null)
				        HashMapUtil.addAL(idsPresent, e.id, Pair.of(e, stbuf.length()-h.start));
				}
				tokenCount += doc.getAnnotations(Token.class, h).size();
				stbuf.append(h.coveredText(doc)).append(sectionTitleSep);
			}
		}
		if (config.titleContext && title != null) {		
			for (EntityWithId e : doc.getAnnotations(EntityWithId.class, title)) {
			    if (e.id != null)
			        HashMapUtil.addAL(idsPresent, e.id, Pair.of(e, stbuf.length()-title.start));
			}
			tokenCount += doc.getAnnotations(Token.class, title).size();
			stbuf.append(title.coveredText(doc)).append(sectionTitleSep);
		}
		for (EntityWithId e : doc.getAnnotations(EntityWithId.class, sent)) {
		    if (e.id != null)
		        HashMapUtil.addAL(idsPresent, e.id, Pair.of(e, stbuf.length()-sent.start));
		}
		
		tokenCount += doc.getAnnotations(Token.class, sent).size();
		
		
		String sentence = stbuf.toString() + sent.coveredText(doc);
		
		if (tokenCount < config.minSentenceTokens || tokenCount > config.maxSentenceTokens)
			return null;
		if (sentence.length() < config.minSentenceChars || sentence.length() > config.maxSentenceChars)
			return null;
		
		//if we are limiting entities to GroundTruth relevant urls, we filter out others here
		if (idsOfInterest != null)
			HashMapUtil.removeIf(idsPresent, e -> !idsOfInterest.contains(e.getKey()));
		
		return sentence;
	}
	
	/**
	 * Build all RelexMentions given the provided sentence and the spans of entities within it.
	 * @param sentence the chunk of text that is the context for the relation mentions
	 * @param idsPresent map from id to EntityWithId and offset adjustment to 'sentence'
	 * @param mentions
	 */
	protected void mentionsFromSentence(String sentence, String docId, Map<String,ArrayList<Pair<EntityWithId,Integer>>> idsPresent, List<RelexMention> mentions) {
		for (List<Pair<EntityWithId,Integer>> e1 : idsPresent.values()) {
			String id1 = e1.get(0).first.id;
			String type1 = e1.get(0).first.type; //CONSIDER: pass the set of types?
			for (List<Pair<EntityWithId,Integer>> e2 : idsPresent.values()) {
				String id2 = e2.get(0).first.id;
				String type2 = e2.get(0).first.type;
				if (id1.compareTo(id2) >= 0) 
					continue;
				if (filter != null && !filter.test(id1, type1, id2, type2))
					continue;

				Collection<String> reltypes = gt != null ? gt.getRelations(id1, id2) : Collections.EMPTY_LIST;
				if (reltypes.isEmpty() && 
						config.negativeExampleSampleFraction < 1.0 && 
					GroundTruth.getDownsamplePriority(id1, id2) > config.negativeExampleSampleFraction) 
				{
				    //CONSIDER: record the downsample
					continue;
				}
				
				//TODO: generalize bestSpanPair to return a List?
				//for each pair of entities
				//find the best spans
				Pair<Integer,Integer> best = bestSpanPair(
						Lists.transform(e1, i -> i.first), 
						Lists.transform(e2, i -> i.first));
				if (best != null) {
					//add a mention						
					Pair<EntityWithId,Integer> e1m = e1.get(best.first);
					Pair<EntityWithId,Integer> e2m = e2.get(best.second);
					addMentions(mentions, e1m, e2m, sentence, docId, reltypes);
				}
			}
		}		
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
		preprocess.process(doc);
		List<RelexMention> mentions = new ArrayList<>();
		Title title = doc.getSingleAnnotation(Title.class);
		Set<Span> toSkip = new HashSet<>();
		//even if the title and sectionheaders are annotated as sentences, we don't view them alone as a source for relation mentions
		if (title != null)
			toSkip.add(new Span(title));
		for (SectionHeader h : doc.getAnnotations(SectionHeader.class)) {
			toSkip.add(new Span(h));
		}
		
		//put the offset adjustment into this map
		Map<String,ArrayList<Pair<EntityWithId,Integer>>> idsPresent = new HashMap<>();
		for (Sentence sent : doc.getAnnotations(Sentence.class)) {
			if (toSkip.contains(new Span(sent))) //we skip the title and sectionheaders, even if marked as sentences.
				continue;
			
			String sentence = getSentenceAndFillIds(doc, title, sent, idsOfInterest, idsPresent);
			
			if (sentence != null)
				mentionsFromSentence(sentence, doc.id, idsPresent, mentions);
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
	
	/**
	 * For non-spark processing of large datasets
	 * @param docs
	 * @param tsvFile
	 */
	public void process(Iterable<Document> docs, File tsvFile) {
		AtomicInteger positiveCount = new AtomicInteger(0);
		AtomicInteger totalCount = new AtomicInteger(0);
		PeriodicChecker report = new PeriodicChecker(100);
		try (RelexMention.Writer writer = new RelexMention.Writer(tsvFile)) {
			BlockingThreadedExecutor threads = new BlockingThreadedExecutor(5);
			for (Document doc : docs) {
			    if (report.isTime()) {
			        System.out.println("CreateTsDataset.process On document "+report.checkCount());
			    }
				threads.execute(() -> {
					List<RelexMention> ms = getMentions(doc);
					totalCount.addAndGet(ms.size());
					for (RelexMention m : ms) {
						if (!m.relTypes.isEmpty())
							positiveCount.addAndGet(1);
						writer.write(m);
					}
				});
			}
			threads.awaitFinishing();
		}
		
		System.out.println("Fraction positive (mention level) = "+((double)positiveCount.get()/totalCount.get())+" total "+positiveCount.get());
		
		//split and sort the file
		GroupRelexMentionTsvDataset.splitAndSort(config, tsvFile);
	}
	
	/**
	 * Example args:
	   configFile groundTruthFile inputDocumentCollection tsvFile
	 * @param args
	 */
	public static void main(String[] args) {
		RelexConfig config = new RelexConfig();
		config.fromString(FileUtil.readFileAsString(args[0]));
		
		GroundTruth gt = FileUtil.loadObjectFromFile(args[1]);
		
		CreateTsvDataset ctd = new CreateTsvDataset(gt, config);
		List<RelexMention> mentions = new ArrayList<>();
		for (Document doc : new DocumentReader(new File(args[2])))
			mentions.addAll(ctd.getMentions(doc));
		ctd.write(mentions, new File(args[3]));
	}
}
