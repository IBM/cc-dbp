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

import com.ibm.research.ai.ki.util.*;

public class RelexDatasetManagerBinary implements IRelexDatasetManager<RelexMention> {
    private static final long serialVersionUID = 1L;

    RelexConfig config;
    GroundTruth gt;
    
    @Override
    public IRelexTsv<RelexMention> getTsvMaker() {
        if (gt == null && new File(config.groundTruthFile).exists())
            this.gt = FileUtil.loadObjectFromFile(config.groundTruthFile);
        if (config.mentionTokenWindowSize > 0)
            return new CreateTsvDatasetTokenWindow(gt, config);
        else
            return new CreateTsvDataset(gt, config);
    }

    @Override
    public IGroundTruth getGroundTruth() {
        if (gt == null && new File(config.groundTruthFile).exists())
            this.gt = FileUtil.loadObjectFromFile(config.groundTruthFile);
        return gt;
    }

    @Override
    public Class<RelexMention> getMentionClass() {
        return RelexMention.class;
    }

    @Override
    public IRelexTensors<RelexMention> getTensorMaker() {
        return new RelexTensors(config);
    }

    static final boolean debugDirectionStyleFixed = false;
    
    @Override
    public void initialize(RelexConfig config) {
        this.config = config;

        if (debugDirectionStyleFixed && config.directionStyle == RelexConfig.DirectionStyle.fixed) {
            List<String> broken = gt.canBeFixedRelationDirection();
            if (!broken.isEmpty()) {
                if (broken.size() > 100)
                    broken = broken.subList(0, 100);
                System.err.println(Lang.stringList(broken, "\n"));
                throw new Error("Relation direction fixed is not workable given this ground truth!");
            }
        }
    }



}
