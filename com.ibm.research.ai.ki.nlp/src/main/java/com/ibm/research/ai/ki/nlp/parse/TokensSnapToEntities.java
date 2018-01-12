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

import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

/**
 * This annotator adjusts the boundaries of tokens to ensure that every entity has a token that starts where the entity starts, and a token that ends where the entity ends.
 * If needed a token is added for the entity.
 * @author mrglass
 *
 */
public class TokensSnapToEntities implements Annotator {
    private static final long serialVersionUID = 1L;

    public static final String SOURCE = TokensSnapToEntities.class.getSimpleName();
    
    @Override
    public void initialize(Properties config) {}
    
    @Override
    public void process(Document doc) {
        //get the non-overlapping spans, preferring longer spans
        List<Entity> nonOverlappingEntities = NonOverlappingSpans.longestSpansNoOverlap(doc.getAnnotations(Entity.class));
        Collections.sort(nonOverlappingEntities);
        //get entities and sort   
        List<Token> toks = doc.getAnnotations(Token.class);
        int tndx = 0;
        List<Token> toAdd = new ArrayList<>();
        for (Entity e : nonOverlappingEntities) {
            //find the first token that ends after the entity starts
            while (tndx < toks.size() && toks.get(tndx).end <= e.start)
                ++tndx;
            
            if (tndx >= toks.size() || toks.get(tndx).start >= e.end) {
                //no token overlaps, we add one
                toAdd.add(new Token(SOURCE, e.start, e.end));
                continue;
            }
            //the first token that overlaps is the start token
            toks.get(tndx).start = e.start;
            
            //find the first token that ends after the entity ends
            while (tndx < toks.size() && toks.get(tndx).end <= e.end)
                ++tndx;
            if (tndx < toks.size() && toks.get(tndx).start < e.end) {
                //this token crosses the entity end boundary, put it entirely inside
                toks.get(tndx).end = e.end;
            } else {
                //this is the last token inside the entity
                toks.get(tndx-1).end = e.end;
            }
            
        }
        for (Token a : toAdd)
            doc.addAnnotation(a);

        if (debug) {
            Set<Integer> tokenStarts = new HashSet<>();
            Set<Integer> tokenEnds = new HashSet<>();
            for (Token t : doc.getAnnotations(Token.class)) {
                tokenStarts.add(t.start);
                tokenEnds.add(t.end);
                if (!t.isValid(doc.text))
                    throw new Error("bad token: "+new Span(t)+" in "+doc.text.length());
            }
            for (Entity e : doc.getAnnotations(Entity.class)) {
                if (!tokenStarts.contains(e.start)) {
                    throw new Error("no start token for "+e.coveredText(doc)+": "+
                    		doc.toSimpleInlineMarkup(
                    				new Span(Math.max(0, e.start-50), Math.min(doc.text.length(), e.end+50)), 
                    				a -> (a instanceof Entity) || (a instanceof Token)));
                }
                if (!tokenEnds.contains(e.end)) {
                    throw new Error("no end token for "+e.coveredText(doc)+": "+
                    		doc.toSimpleInlineMarkup(
                    				new Span(Math.max(0, e.start-50), Math.min(doc.text.length(), e.end+50)), 
                    						a -> (a instanceof Entity) || (a instanceof Token)));
                }
            }
        }
    }
    private static final boolean debug = true;

}
