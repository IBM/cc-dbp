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
package com.ibm.research.ai.ki.nlp.parse;

import java.io.*;
import java.util.*;

import opennlp.tools.chunker.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class OpenNLPChunk implements Annotator {
	private static final long serialVersionUID = 1L;
	
	public static final String openNLPChunkModel = "openNLPChunkModel";
	public static final String defaultOpenNLPChunkModel = "en-chunker.bin";
	//public static final String skipNoSentence = "skipNoSentence";
	public static final String SOURCE = OpenNLPChunk.class.getSimpleName();
	
	protected ChunkerModel model;
	
	protected transient ThreadLocal<ChunkerME> chunkert;
	
	@Override
	public void initialize(Properties config) {
		try {
			String modelLocation = Lang.NVL(config.getProperty(openNLPChunkModel), defaultOpenNLPChunkModel);
			InputStream is = FileUtil.getResourceAsStream(modelLocation);
			if (is == null)
				throw new Error("cannot find "+modelLocation);
			model = new ChunkerModel(is);
			is.close();
			chunkert = new ThreadLocal<ChunkerME>() {
				public ChunkerME initialValue() {
					return new ChunkerME(model);
				}};
		} catch (Exception e) {
			Lang.error(e);
		}
	}

	private static Set<String> badEnds = new HashSet<>(Arrays.asList(".", "?", "'", "\""));
	
	//wrapper to add a few filters on the chunks we add
	protected void addChunk(Document doc, List<Token> tokens, String tag, int startTok, int endTok) {
		//strip non-word tokens from end
		//while (endTok > 0 && badEnds.contains(tokens.get(endTok-1).coveredText(doc)))
		//	--endTok;
		//remove possesive from start of chunk
		while (startTok < endTok && tokens.get(startTok).coveredText(doc).equalsIgnoreCase("'s"))
			++startTok;
		if (endTok > startTok)
			doc.addAnnotation(new Chunk(SOURCE, tokens.get(startTok).start, tokens.get(endTok-1).end, tag));
	}
	
	@Override
	public void process(Document doc) {
		ChunkerME chunker = chunkert.get();
		for (Annotation segment : doc.getAnnotations(Sentence.class)) {
			List<Token> tokens = doc.getAnnotations(Token.class, segment);
			if (tokens.isEmpty())
				continue;
			String[] words = new String[tokens.size()];
			String[] pos = new String[tokens.size()];
			for (int i = 0; i < tokens.size(); ++i) {
				words[i] = tokens.get(i).coveredText(doc);
				pos[i] = tokens.get(i).pos;
				if (pos[i] == null) {
					//CONSIDER: how to handle??
					//pos[i] = "NN";
					throw new Error("no POS! '"+words[i]+"' in ``"+segment.coveredText(doc)+"''");
				}
			}
			String[] chunkTags = chunker.chunk(words, pos);
			//now create chunk annotations			
			int tstart = -1;
			String curTag = null;
			for (int i = 0; i < chunkTags.length; ++i) {
				char bio = chunkTags[i].charAt(0);
				if (curTag != null && bio == 'O') {
					addChunk(doc, tokens, curTag, tstart, i);
					curTag = null;
				} else if (bio == 'B') {
					//CONSIDER: often get this wrong: 'many types of X' 'loss of X'
					// try converting the pattern 'NP of NP' -> NP
					// see how that does
					if (curTag != null) {
						addChunk(doc, tokens, curTag, tstart, i);
					}
					tstart = i;
					curTag = chunkTags[i].substring(2);
				} //ok to ignore I
			}
			if (curTag != null) {
				addChunk(doc, tokens, curTag, tstart, tokens.size());
			}
		}
	}

	/**
	 * Examine cases where chunking gives 'NP of NP'.
	 * Maybe most should be a single NP?
	 * @param docDir
	 */
	private static void npOfNPPattern(File docDir) {
		RandomUtil.Sample<String> nps = new RandomUtil.Sample<>(100);
		for (Document doc : new DocumentReader(docDir)) {
			for (Sentence sent : doc.getAnnotations(Sentence.class)) {
				Chunk phrasalDet = null;
				Chunk of = null;
				Chunk np = null;
				for (Chunk c : doc.getAnnotations(Chunk.class, sent)) {
					if (phrasalDet == null && "NP".equals(c.tag)) {
						phrasalDet = c;
					} else if (phrasalDet != null && of == null && c.coveredText(doc).equalsIgnoreCase("of")) {
						of = c;
					} else if (of != null && np == null && "NP".equals(c.tag)) {
						//record it
						nps.maybeSave(doc.text.substring(phrasalDet.start, c.end));
					} else {
						phrasalDet = null;
						of = null;
						np = null;
					}
				}
			}
		}
		System.out.println(Lang.stringList(nps, "\n"));
	}
	
	private static void test() {
		Document doc = new Document(
				"He reckons the current account deficit will narrow to only 1.8 billion in September.\n"
				+ "Seek medical care if your neck pain is accompanied by numbness or loss of strength in your arms or hands or if you have shooting pain into your shoulder or down your arm.\n"
				+ "Studies have found that acupuncture may be helpful for many types of pain.\n"
				+ "Little scientific evidence exists to support massage in people with neck pain, though it may provide relief when combined with your doctor's recommended treatments.");
		Pipeline p = new Pipeline(new RegexParagraph(), new OpenNLPSentence(), 
				new OpenNLPTokenize(), new OpenNLPPOS(), 
				//new ClearNLPTokenize(), new ClearNLPPOS(), //either one works about as well as the other
				new OpenNLPChunk());
		p.initialize(new Properties());
		p.process(doc);
		System.out.println(doc.highlight(Chunk.class));
	}
	
	public static void main(String[] args) {
		//test();
		//npOfNPPattern(new File(args[0]));
		}
}
