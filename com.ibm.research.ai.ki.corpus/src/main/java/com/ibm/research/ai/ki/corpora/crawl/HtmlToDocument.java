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
package com.ibm.research.ai.ki.corpora.crawl;

import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;

import com.kohlschutter.boilerpipe.document.*;
import com.kohlschutter.boilerpipe.extractors.*;

public class HtmlToDocument {
    public static final int minTextBlockWords = 5; //5 is a good default
    
    ExtractorBase extractor = new KeepEverythingWithMinKWordsExtractor(minTextBlockWords);
    
    public Document toDocument(String uri, String html) {
        try {
            List<BPAnnotation> annos = new ArrayList<>();
            TextDocument td = extractor.getTextDocument(html);
            String text = td.getText(true, false, annos);
            String title = td.getTitle();
            if (text == null || text.isEmpty())
                return null;
            if (title == null) {
                title = "";
            } else if (title.length() > 0) {
                title = title+"\n\n";
            }
            Document doc = new Document(uri, title+text);
            if (title.length() > 0)
                doc.addAnnotation(new Title(null, 0, title.length()-2));
            for (BPAnnotation l : annos) {
                Annotation a = null;
                if (l instanceof Link) {
                    a = new LinkAnnotation(null, l.start, l.end, ((Link)l).href);
                } else if (l instanceof HeaderAnnotation) {
                    a = new SectionHeader(null, l.start, l.end);
                } else if (l instanceof TextFormatAnnotation) {
                    a = new TextFormatting(null, 
                            l.start, l.end,
                            l.localName.equals("i") ? TextFormatting.Format.italic : TextFormatting.Format.bold); 
                } else if (l instanceof ParagraphAnnotation) {
                    a = new Paragraph(null, l.start, l.end);
                }
                if (a != null) {
                    a.addOffset(title.length());
                    doc.addAnnotation(a);
                }
            }
            return doc;
        } catch (Throwable t) {
            return null;
        }
    }
}
