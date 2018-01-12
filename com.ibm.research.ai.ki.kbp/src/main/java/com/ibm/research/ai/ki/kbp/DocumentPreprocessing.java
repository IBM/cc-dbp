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

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

public class DocumentPreprocessing {
    public Pipeline getPipeline(RelexConfig config, IGroundTruth gt) {
        List<Annotator> annos = new ArrayList<>();
        // if config.entityRecognitionPostProcess is specified, post process the
        // documents
        try {
            if (config.entityRecognitionPostProcess != null) {
                IPostprocessEntityRecognition[] postProcess = new IPostprocessEntityRecognition[config.entityRecognitionPostProcess.length];
                for (int i = 0; i < postProcess.length; ++i) {
                    postProcess[i] = (IPostprocessEntityRecognition) Class.forName(
                            config.entityRecognitionPostProcess[i]).newInstance();
                    postProcess[i].initialize(gt, config);
                }
                annos.addAll(Arrays.asList(postProcess));
            }
        } catch (Exception e) {
            Lang.error(e);
        }
     
        // filter the entities by ground truth, if not already handled by
        // entityRecognitionPostProcess
        if (config.limitEntitiesToGroundTruth
                && (config.entityRecognitionPostProcess == null || Lang.linearSearch(
                        config.entityRecognitionPostProcess, FilterEntsByGroundTruth.class.getName()) == -1)) 
        {
            // filter to include only DBpedia entities of interest
            FilterEntsByGroundTruth filter = new FilterEntsByGroundTruth();
            filter.initialize(gt, config);
            annos.add(filter);
        }

        return new Pipeline(annos);
    }

}
