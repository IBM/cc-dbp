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

import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;

public class RelexDatasetManagerUnary implements IRelexDatasetManager<UnaryRelexMention> {
    private static final long serialVersionUID = 1L;

    RelexConfig config;
    UnaryGroundTruth gt;
    
    @Override
    public IRelexTsv<UnaryRelexMention> getTsvMaker() {
        if (gt == null && new File(config.groundTruthFile).exists())
            this.gt = FileUtil.loadObjectFromFile(config.groundTruthFile);
        return new UnaryRelexTsvDataset(gt, config);
    }

    @Override
    public IGroundTruth getGroundTruth() {
        if (gt == null && new File(config.groundTruthFile).exists())
            this.gt = FileUtil.loadObjectFromFile(config.groundTruthFile);
        return gt;
    }

    @Override
    public Class<UnaryRelexMention> getMentionClass() {
        return UnaryRelexMention.class;
    }

    @Override
    public IRelexTensors<UnaryRelexMention> getTensorMaker() {
        return new UnaryRelexTensors(config);
    }

    @Override
    public void initialize(RelexConfig config) {
        this.config = config;
        
    }


}
