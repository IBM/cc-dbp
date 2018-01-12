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

import java.util.List;

import com.ibm.research.ai.ki.util.*;

import com.optimaize.langdetect.*;
import com.optimaize.langdetect.i18n.*;
import com.optimaize.langdetect.ngram.*;
import com.optimaize.langdetect.profiles.*;
import com.optimaize.langdetect.text.*;

/**
 * Using: https://github.com/optimaize/language-detector
 * 
 *  Alternatives at:
    https://code.google.com/archive/p/language-detection/
    or
    http://tika.apache.org/1.15/api/
      LangaugeDetector
 * @author mrglass
 *
 */
public class LanguageScorer {

    protected final String forLanguage;
    protected LanguageDetector languageDetector;
    protected TextObjectFactory textObjectFactory;
    
    public LanguageScorer(String forLanguage) {
        this.forLanguage = forLanguage;
        try {
            // load all languages:
            List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

            // build language detector:
            languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                    .withProfiles(languageProfiles).build();

            // create a text object factory
            textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
        } catch (Exception e) {
            Lang.error(e);
        }
    }

    public double score(String text) {
        // query:
        TextObject textObject = textObjectFactory.forText(text);
        //Optional<LdLocale> lang = languageDetector.detect(textObject);
        List<DetectedLanguage> langs = languageDetector.getProbabilities(textObject);
        for (DetectedLanguage lang : langs) {
            LdLocale l = lang.getLocale();
            if (forLanguage.equals(l.getLanguage()))
                return lang.getProbability();
        }
        return 0.0;
    }

}
