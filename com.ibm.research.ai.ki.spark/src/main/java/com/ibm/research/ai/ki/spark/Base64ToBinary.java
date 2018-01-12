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
package com.ibm.research.ai.ki.spark;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.FileUtil;
import com.ibm.research.ai.ki.util.io.*;

public class Base64ToBinary {
	
	static class MultiFileDataOut extends MultiFileWriter<DataOutputStream, byte[]> {
		public MultiFileDataOut(File localDir, RelexConfig config) {
			super(localDir, config.tensorInstancesPerFile, false);
		}
		
		@Override
		protected String getExt() {
			return ".bin.gz";
		}

		@Override
		protected void write(DataOutputStream stream, byte[] obj) throws IOException {
			stream.write(obj);
		}

		@Override
		protected DataOutputStream getStream(File f) throws IOException {
			return FileUtil.getDataOutput(f.getAbsolutePath());
		}
	}
	
	protected static String getId(byte[] tensorSet) {
		try (DataInputStream bais = new DataInputStream(new ByteArrayInputStream(tensorSet))) {
			bais.readInt(); //num tensors
			bais.readByte(); //type = String
			bais.readInt(); //order of String tensor
			return DataIO.readUTFSimpler(bais);
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	//CONSIDER: this part could also do the splitting into train/validate/test
	@SafeVarargs
	public static void convert(RelexConfig config, FileSystem fs, String hdfsDir, Pair<File,Double>... partitionDirs) {
		try {
			long startTime = System.currentTimeMillis();
			
			double[] cdf = new double[partitionDirs.length];
			MultiFileDataOut[] outs = new MultiFileDataOut[partitionDirs.length];
			double sumProb = 0;
			for (int i = 0; i < partitionDirs.length; ++i) {
				sumProb += partitionDirs[i].second;
				cdf[i] = sumProb;
				outs[i] = new MultiFileDataOut(partitionDirs[i].first, config);
			}
			if (Math.abs(sumProb - 1.0) > 0.00001)
				throw new IllegalArgumentException("partition fractions must sum to one!");
			cdf[cdf.length-1] = 1.0;
			
			double[] splitCounts = new double[outs.length];
			
			PeriodicChecker report = new PeriodicChecker(100);
			RemoteIterator<LocatedFileStatus> fit = fs.listFiles(new Path(hdfsDir), true);
			while (fit.hasNext()) {
				Path p = fit.next().getPath();
				if (p.getName().equals("_SUCCESS"))
					continue;
				
				FSDataInputStream in = fs.open(p);
				BufferedReader r = new BufferedReader(new InputStreamReader(in));
				String line = null;
				while ((line = r.readLine()) != null) {
					if (report.isTime()) {
						System.out.println("Converting line "+report.checkCount()+", "+Lang.milliStr(report.elapsedTime()));
					}
					try {
						byte[] ts = Base64.getDecoder().decode(line);
						String id = getId(ts);
						int splitNdx = RandomUtil.sampleFromCDF(cdf, GroundTruth.getSplitLocation(id));
						splitCounts[splitNdx] += 1;
						outs[splitNdx].write(ts);
					} catch (Exception e) {
						throw new Error("On "+p.toString()+" line "+report.checkCount()+":\n"+line, e);
					}
				}
				try {
				r.close();
				in.close();
				} catch (Exception e) {}
			}
			for (MultiFileDataOut out : outs)
				out.close();
			DenseVectors.scale(splitCounts, 1.0/DenseVectors.oneNorm(splitCounts));
			for (int i = 0; i < partitionDirs.length; ++i) {
			    System.out.println("In "+partitionDirs[i].first.getName()+" targeted "+partitionDirs[i].second+" actual "+splitCounts[i]);
			}
			System.out.println("gathered and base64 decoded in "+Lang.milliStr(System.currentTimeMillis()-startTime));
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	
	/*
	private static void readFile() {
		 try {
		        Configuration conf = new Configuration();
		        conf.set("fs.defaultFS", "hdfs://localhost:54310/user/hadoop/");
		        FileSystem fs = FileSystem.get(conf);
		        FileStatus[] status = fs.listStatus(new Path("hdfsdirectory"));
		        for(int i=0;i<status.length;i++){
		            System.out.println(status[i].getPath());
		            fs.copyToLocalFile(false, status[i].getPath(), new Path("localdir"));
		            RemoteIterator<LocatedFileStatus> fit = fs.listFiles(null, true);
		            //fit.next().getPath()
		            FSDataInputStream in = fs.open(null);
		            //TODO: new BufferedReader(new InputStreamReader(in)).readLine()
		        }
		    } catch (IOException e) {
		        e.printStackTrace();
		    }
	}
	*/
	
	/**
	 * Converts Base64 encoded binary format to binary.
	 * Used to convert the output of Spark RelexTensorDataset to TensorFileReader binary format.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String in = args[0];
		
		for (File f : new FileUtil.FileIterable(new File(in))) {
			if (f.getName().equals("_SUCCESS")) {
				f.delete();
				continue;
			}
			DataOutputStream out = FileUtil.getDataOutput(f.getAbsolutePath()+".bin");
			for (String line : FileUtil.getLines(f.getAbsolutePath())) {
				out.write(Base64.getDecoder().decode(line));
			}
			out.close();
			f.delete();
		}
	}
}
