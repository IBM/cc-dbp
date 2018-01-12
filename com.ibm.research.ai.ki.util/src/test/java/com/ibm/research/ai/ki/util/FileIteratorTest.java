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
package com.ibm.research.ai.ki.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.FileUtil.*;

public class FileIteratorTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  @Before
  public void setUp() throws Exception {
    folder.newFile("1.txt");
    folder.newFile("b.txt");
    folder.newFile("remove.txt");
    folder.newFile("a.xls");

    File f_1g1 =  folder.newFolder("1G1");
    File f_1g2 =  folder.newFolder("1G2");
    File f_2g1 = new File(f_1g2.getAbsolutePath()+File.separator+"2G1");
    f_2g1.mkdirs();

    new File(f_1g1.getAbsolutePath()+File.separator+"c.txt").createNewFile();
    new File(f_1g1.getAbsolutePath()+File.separator+"d.txt").createNewFile();
    new File(f_1g1.getAbsolutePath()+File.separator+"e.txt").createNewFile();
    new File(f_1g2.getAbsolutePath()+File.separator+"f.txt").createNewFile();
    new File(f_1g2.getAbsolutePath()+File.separator+"g.txt").createNewFile();
    new File(f_1g2.getAbsolutePath()+File.separator+"h.txt").createNewFile();
    new File(f_2g1.getAbsolutePath()+File.separator+"j.txt").createNewFile();
    new File(f_2g1.getAbsolutePath()+File.separator+"b.xls").createNewFile();
  }

  @After
  public void tearDown() throws Exception {
   folder.delete();
  }

  @Test
  public void test() {
    List<File> normal = new ArrayList<File>();
    FileUtil.FileIterator normalIteration = new FileIterator(folder.getRoot(), null, null);
    while (normalIteration.hasNext()){
      File file = normalIteration.next();
      System.out.println(file);
      normal.add(file);
    }
    
    List<File> filtered = new ArrayList<File>();
    FileUtil.FileIterator filteredIteration = new FileIterator(folder.getRoot(), getFilter(), null);
    while (filteredIteration.hasNext()){
      filtered.add(filteredIteration.next());
    }
    assertEquals(2, filtered.size());
    assertEquals(12, normal.size());
    assertEquals("b.xls", normal.get(10).getName());
  }
  
  private FileFilter getFilter(){
    FileFilter filter = new FileFilter() {
      @Override
      public boolean accept(File file) {
        if (file.getName().endsWith(".xls")){
          return true;
        }
        return false;
      }
    };
    return filter;
  }
}
