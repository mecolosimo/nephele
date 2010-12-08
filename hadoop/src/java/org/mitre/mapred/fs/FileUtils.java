/**
 * Created on March 23, 2009.
 *
 * Copyright 2010- The MITRE Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you  may not 
 * use this file except in compliance with the License. You may obtain a copy of 
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions andlimitations under
 * the License.
 *
 * $Id$
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mitre.mapred.fs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 * A collection of file-processing util methods.
 *
 * @author Marc Colosimo
 */
public class FileUtils extends FileUtil {

    /**
     * Returns a tmp path on the remote FileSystem.
     *
     * @param fs
     * @param basePath
     * @return The path
     * @throws java.io.IOException
     */
    public static final Path createRemoteTempPath(FileSystem fs, Path basePath) throws IOException {

        long now = System.currentTimeMillis();
        // @TODO: add constant and look up tmp dir name
        Path tmpDirPath = new Path(basePath.toString() + Path.SEPARATOR +
                "tmp_" + Long.toHexString(now));
        // check to see if unqiue?
        return fs.makeQualified(tmpDirPath);
    }

    public static String convertStreamToString(InputStream is) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        StringBuilder sb = new StringBuilder();
        String line = null;

        try {

            while ((line = reader.readLine()) != null) {
                // This will add a newline if the last line ends at EOF without a newline
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

    /**
     * Get a listing of all files that match the file pattern <i>srcf</i>.
     * <P>Example: "part-*" should return all the parts in lex order</P>
     * 
     * @param srcf a file pattern specifying source files
     * @throws IOException
     * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
     * @see org.apache.hadoop.fs.FsShell
     */
    public static synchronized Path[] ls(JobConf conf, String srcF) throws IOException {
        Path srcPath = new Path(srcF);
        FileSystem srcFs = srcPath.getFileSystem(conf);
        FileStatus[] srcs = srcFs.globStatus(srcPath);
        if (srcs == null || srcs.length == 0) {
            throw new FileNotFoundException("Cannot access " + srcPath.toString() +
                    ": No such file or directory.");
        }

        Path[] srcP = new Path[srcs.length];
        for (int i = 0; i < srcs.length; i++) {
            FileStatus stat = srcs[i];
            srcP[i] = stat.getPath();
        }
        return srcP;
    }

    /**
     * Generate the current date/time in the pattern "yyyyddHHmmssSSS" (i.e., 200902260829)
     */
    public static String getSimpleDate() {
        Calendar calendar = Calendar.getInstance();
        return new SimpleDateFormat("yyyyddHHmmssSSS").format(calendar.getTime());
    }

    /**
     * If <code>libjars</code> is set in the conf, parse the libjars URIs to URLs.
     * @see org.apache.hadoop.util.GenericOptionsParser#getLibJars(org.apache.hadoop.conf.Configuration)
     * @param conf
     * @return libjar {@link URL}s
     * @throws IOException
     */
    public static URL[] getLibJars(Configuration conf) throws IOException {
        String jars = conf.get("tmpjars");
        if (jars == null) {
            return null;
        }
        String[] files = jars.split(",");
        URL[] cp = new URL[files.length];
        for (int i = 0; i < cp.length; i++) {
            Path tmp = new Path(files[i]);
            cp[i] = FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL();
        }
        return cp;
    }

    /**
     * Takes input as a comma separated list of files
     * and verifies if they exist. It defaults for file:///
     * if the files specified do not have a scheme.
     * it returns the paths uri converted defaulting to file:///.
     * So an input of  /home/user/file1,/home/user/file2 would return
     * file:///home/user/file1,file:///home/user/file2
     *
     * @see org.apache.hadoop.util.GenericOptionsParser#validateFiles(java.lang.String, org.apache.hadoop.conf.Configuration)
     * @param files
     * @return the paths converted to URIs
     */
    public static String validateFiles(String files, Configuration conf) throws IOException {
        if (files == null) {
            return null;
        }
        String[] fileArr = files.split(",");
        String[] finalArr = new String[fileArr.length];
        for (int i = 0; i < fileArr.length; i++) {
            String tmp = fileArr[i];
            String finalPath;
            Path path = new Path(tmp);
            URI pathURI = path.toUri();
            FileSystem localFs = FileSystem.getLocal(conf);
            if (pathURI.getScheme() == null) {
                //default to the local file system
                //check if the file exists or not first
                if (!localFs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }
                finalPath = path.makeQualified(localFs).toString();
            } else {
                // check if the file exists in this file system
                // we need to recreate this filesystem object to copy
                // these files to the file system jobtracker is running
                // on.
                FileSystem fs = path.getFileSystem(conf);
                if (!fs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }
                finalPath = path.makeQualified(fs).toString();
                try {
                    fs.close();
                } catch (IOException e) {
                }
            }
            finalArr[i] = finalPath;
        }
        return StringUtils.arrayToString(finalArr);
    }
}
