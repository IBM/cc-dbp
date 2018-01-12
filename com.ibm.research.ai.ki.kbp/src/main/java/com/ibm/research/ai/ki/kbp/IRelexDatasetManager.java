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

import com.ibm.research.ai.ki.kbp.*;

/**
 * Provides classes for representing and creating a dataset for training/evaluation/mass-apply of 
 * a relational knowledge induction system.
 * 
 * @author mrglass
 *
 * @param <M>
 */
public interface IRelexDatasetManager<M extends IRelexMention> extends Serializable {
    
    public IRelexTsv<M> getTsvMaker();
    public IGroundTruth getGroundTruth();
    public Class<M> getMentionClass();
    public IRelexTensors<M> getTensorMaker();
    
    /**
     * before this method is called, only getMentionClass is supposed to be called
     * @param config
     */
    public void initialize(RelexConfig config);
}