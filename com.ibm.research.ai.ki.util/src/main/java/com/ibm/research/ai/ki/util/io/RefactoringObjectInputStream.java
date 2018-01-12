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
/*
Copyright (c) 2012 IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.ibm.research.ai.ki.util.io;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.util.*;

public class RefactoringObjectInputStream extends ObjectInputStream {
	//refactored.put("oldpackage.Classname", newpackage.Classname.class);
	private static Map<String,Class> refactored = null;
	private static boolean oldVersionOfMappingFound = false;
	//static initialize from properties file: serializedMappings.properties
	static {
		refactored = new HashMap<String,Class>();
		Properties props = PropertyLoader.loadProperties("com.ibm.research.ai.ki.util.serializedMappings");

		for (String key : props.stringPropertyNames()) {
			try {
				//support refactored per-serial version id
				int colonNdx = key.indexOf(':');
				if (colonNdx != -1) {
					//just a marker to indicate that some versions of this class have remappings dependent on serialVersionId
					refactored.put(key.substring(0, colonNdx), OldVersionOf.class); 
					oldVersionOfMappingFound = true;
				}
				refactored.put(key, Class.forName((String)props.get(key)));
			} catch (ClassNotFoundException e) {
				//System.err.println("RefactoringObjectInputStream: in com.ibm.bluej.commonutil.serializedMappings, not found: "+key+" to "+props.get(key));
			}
		}
	}
	
    public RefactoringObjectInputStream(InputStream in) throws IOException {
        super(in);
        if (oldVersionOfMappingFound)
        	super.enableResolveObject(true);
    }

    
    @Override
    public Object resolveObject(Object o) {
    	if (o instanceof OldVersionOf) {
    		o = ((OldVersionOf)o).convert();
    	}
    	return o;
    }
    
    
    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass resultClassDescriptor = super.readClassDescriptor();
        if (refactored.size() > 0) {
	        Class remap = refactored.get(resultClassDescriptor.getName());
	        if (remap != null) {
	        	if (remap == OldVersionOf.class) {
	        		long id = resultClassDescriptor.getSerialVersionUID();
		        	remap = refactored.get(resultClassDescriptor.getName()+":"+id);
		        	if (remap != null)
		        		resultClassDescriptor = ObjectStreamClass.lookup(remap);
	        	} else {
	        		resultClassDescriptor = ObjectStreamClass.lookup(remap);
	        	}
	        }
        }
        return resultClassDescriptor;
    }
	
}
