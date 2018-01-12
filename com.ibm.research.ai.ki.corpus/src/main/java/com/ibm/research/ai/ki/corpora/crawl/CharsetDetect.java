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

import java.io.*;
import java.nio.charset.*;

import org.mozilla.universalchardet.*;

public class CharsetDetect {
    static String mapCharset(String charsetName) {
        try {
            if (Charset.isSupported(charsetName))
                return charsetName;
            String lc = charsetName.toLowerCase();
            if(lc.contains("iso8859-1") || lc.contains("iso-8859-1")) {
                return "cp1252";
            }
            return charsetName;
        } catch (Throwable t) {
            return "UTF-8";
        }
    }
    
    public static String getCharsetFromBytes(byte buffer[]) throws IOException {
        UniversalDetector detector = new UniversalDetector(null);
        detector.handleData(buffer, 0, buffer.length);
        detector.dataEnd();
        String charsetName = detector.getDetectedCharset();
        detector.reset();
        return mapCharset(charsetName);
    }
}
