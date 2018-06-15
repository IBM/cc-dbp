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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.parallel.*;

/**
 * 
 * @author mrglass
 *
 */
public class UnaryRelexTsvDataset implements IRelexTsv<UnaryRelexMention> {
	private static final long serialVersionUID = 1L;
	
	protected UnaryGroundTruth gt;
	protected RelexConfig config;
	protected Annotator preprocess;
	public final IEntityFilter filter;
	
	public UnaryRelexTsvDataset(UnaryGroundTruth gt, RelexConfig config) {
		this.gt = gt;
		this.config = config;
		if (config.entityPairFilterClass != null) {
            try {
                filter = (IEntityFilter)Class.forName(config.entityPairFilterClass).newInstance();
                filter.initialize(gt, config);
                System.out.println("Filtering entities by "+config.entityPairFilterClass);
            } catch (Exception e) {
                throw new Error(e);
            }
        } else {
            filter = null;
        }
		preprocess = new DocumentPreprocessing().getPipeline(config, gt);
	}
	
	protected List<UnaryRelexMention> tokenWindowMentions(Document doc) {
	    List<Token> tokens = doc.getAnnotations(Token.class);
	    List<UnaryRelexMention> mentions = new ArrayList<>();
        for (EntityWithId ent : doc.getAnnotations(EntityWithId.class)) {
            String[] rels = null;
            String id = Lang.NVL(ent.id, ent.coveredText(doc).toLowerCase());
            String type = Lang.NVL(ent.type, GroundTruth.unknownType);
            if (filter != null && !filter.test(doc.id, id, type))
                continue;
            if (ent.id != null) 
                rels = gt.getRelations(ent.id);
            if ((rels == null || rels.length == 0) && 
                config.negativeExampleSampleFraction < 1.0 && 
                GroundTruth.getSingleDownsamplePriority(id) > config.negativeExampleSampleFraction) 
            {
                continue;
            }
            
            Span tokSpan = Span.toSegmentSpan(ent, tokens);
            int startTokNdx = Math.max(0, tokSpan.start-config.mentionTokenWindowSize);
            int endTokNdx = Math.min(tokens.size(), tokSpan.end+config.mentionTokenWindowSize)-1;
            Span sentSpan = new Span(tokens.get(startTokNdx).start, tokens.get(endTokNdx).end);
            String sentStr = sentSpan.substring(doc.text);
            
            Span s = new Span(ent);
            s.addOffset(-tokens.get(startTokNdx).start);
            UnaryRelexMention um = new UnaryRelexMention(
                    id, type, s, 
                    rels, 
                    sentStr,
                    doc.id,
                    sentSpan);
            if (config.placeholdersForArguments)
                um.convertToPlaceholders();
            mentions.add(um);
        }
        return mentions;
	}
	
	protected List<UnaryRelexMention> sentenceMentions(Document doc) {
	    List<UnaryRelexMention> mentions = new ArrayList<>();
        
        for (Sentence sent : doc.getAnnotations(Sentence.class)) {
            if (sent.length() < config.minSentenceChars || sent.length() > config.maxSentenceChars)
                continue;
            int tokenCount = doc.getAnnotations(Token.class, sent).size();
            if (tokenCount < config.minSentenceTokens || tokenCount > config.maxSentenceTokens)
                continue;
            String sentStr = sent.coveredText(doc).replace('\t', ' ').replace('\n', ' ');
            for (EntityWithId ent : doc.getAnnotations(EntityWithId.class, sent)) {
                String[] rels = null;
                String id = Lang.NVL(ent.id, ent.coveredText(doc).toLowerCase());
                String type = Lang.NVL(ent.type, GroundTruth.unknownType);
                if (filter != null && !filter.test(doc.id, id, type))
                    continue;
                if (ent.id != null) 
                    rels = gt.getRelations(ent.id);
                if ((rels == null || rels.length == 0) && 
                    config.negativeExampleSampleFraction < 1.0 && 
                    GroundTruth.getSingleDownsamplePriority(id) > config.negativeExampleSampleFraction) 
                {
                    continue;
                }

                Span s = new Span(ent);
                s.addOffset(-sent.start);
                UnaryRelexMention um = new UnaryRelexMention(
                        id, type, s, 
                        rels, 
                        sentStr,
                        doc.id,
                        new Span(sent));
                if (config.placeholdersForArguments)
                    um.convertToPlaceholders();
                mentions.add(um);
            }
        }
        return mentions;
	}
	
	public List<UnaryRelexMention> getMentions(Document doc) {
		if (doc == null)
			return Collections.emptyList();
		preprocess.process(doc);
		
		if (config.mentionTokenWindowSize > 0) {
		    return tokenWindowMentions(doc);
		} else {
		    return sentenceMentions(doc);
		}
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
        //TODO: maybe this should do multifile?
        try (PrintStream writer = FileUtil.getFilePrintStream(tsvFile.getAbsolutePath())) {
            BlockingThreadedExecutor threads = new BlockingThreadedExecutor(5);
            for (Document doc : docs) {
                if (report.isTime()) {
                    System.out.println("UnaryRelexTsvDataset.process On document "+report.checkCount());
                }
                threads.execute(() -> {
                    List<UnaryRelexMention> ms = getMentions(doc);
                    totalCount.addAndGet(ms.size());
                    for (UnaryRelexMention m : ms) {
                        if (!m.isNegative())
                            positiveCount.addAndGet(1);
                        String mstr = m.toString();
                        synchronized (writer) {
                            writer.println(mstr);
                        }
                    }
                });
            }
            threads.awaitFinishing();
        }
        
        System.out.println("Fraction positive (mention level) = "+((double)positiveCount.get()/totalCount.get())+" total "+positiveCount.get());
        
        //split and sort the file
        GroupRelexMentionTsvDataset.splitAndSort(config, tsvFile);
    }	
}
