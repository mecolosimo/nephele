/**
 * Created Feb 25, 2009.
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
package org.mitre.bio.mapred.io;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

/**
 * A FASTA file {@link RecordReader}.
 *
 * <P>FASTA sequence records start with '>' on a line with the following lines
 * containing the sequence. Keys are the position in the file and
 * values are one FASTA record (header plus sequence). </P>
 *
 * <P>NOTE: in 0.19.0 there seems to be multiple RecordReader classes:
 * one is an interface the other is an abstract class.</P>
 *
 * @author Marc Colosimo
 */
public class FastaRecordReader implements RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(FastaRecordReader.class);
    private static final String startTag = ">";
    private long start;
    private long end;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private LineReader in;
    private int maxLineLength;
    private CompressionCodecFactory compressionCodecs = null;
    private long pos;
    private String pushBackString;
    private int pushBackSize;

    public FastaRecordReader(FileSplit split, JobConf job) throws IOException {
        this.pushBackString = null;
        this.pushBackSize = 0;

        this.maxLineLength = job.getInt("io.file.buffer.size", // mapred.linereader.maxlength
                Integer.MAX_VALUE);

        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final Path file = split.getPath();

        this.compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = this.compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            this.in = new LineReader(codec.createInputStream(fileIn), job);
            this.end = Long.MAX_VALUE;
        } else {
            /**
             * From LineRecordReader, what is this doing?
             */
            if (this.start != 0) {
                LOG.info("Skipping first line in split");
                skipFirstLine = true;
                --this.start;
                fileIn.seek(this.start);
            }
            this.in = new LineReader(fileIn, job);
        }
        if (skipFirstLine) {
            /**
             * Skipping first line to re-established "start".
             */
            this.start += in.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    /**
     * Reads the next key/value pair from the input for processing.
     *
     * @param key the key to read data into
     * @param value the value to read data into
     * @return true iff a key/value was read, false if at EOF
     */
    @Override
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        this.buffer.reset();
        if (this.pos < this.end) {
            try {
                // Find the being of a new record block
                if (readLinesUntilStartsWithMatch(startTag, false)) {
                    // Read until we find the endTag or EOF
                    readLinesBeforeStartsWithMatch(startTag, true);
                    if (buffer.size() > 0) {
                        key.set(this.pos);
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                }
            } finally {
                LOG.debug("Uncaught exception!");
                this.buffer.reset();
            }
        }
        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public synchronized long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }

    /**
     * Get the progress within the split
     */
    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    /**
     * Read lines from the InputStream until a line starts with the matchin string.
     *
     * @param match the bytes to match the begining of a line with
     * @param withinBlock if true we are saving bytes as we read (this.buffer)
     * @return true if we found the match
     * @throws java.io.IOException
     */
    private boolean readLinesUntilStartsWithMatch(String matchString,
            boolean withinBlock) throws IOException {
        Text value;
        int newSize;
        if (this.pushBackString != null) {
            value = new Text(this.pushBackString);
            //newSize = this.pushBackString.getBytes().length;
            newSize = this.pushBackSize;
            this.pushBackSize = 0;
            this.pushBackString = null;
        } else {
            value = new Text();
            newSize = in.readLine(value, maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                    maxLineLength));
        }
        while (newSize != 0) {
            this.pos += newSize;
            String line = value.toString();
            if (line.length() != 0 && line.startsWith(matchString)) {
                // Return this line and true
                buffer.write(line.getBytes());
                buffer.write("\n".getBytes());
                return true;
            }

            if (withinBlock) {
                buffer.write(line.getBytes());
                buffer.write("\n".getBytes());
            }

            newSize = in.readLine(value, this.maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE,
                    this.end - this.pos),
                    this.maxLineLength));
        }
        return false;
    }

    /**
     * Reads the split (InputStream) up until a line starting with the
     * <code>matchString</code> leaving that line in pushBackString for the next time around.
     *
     * @param matchString   String to match the begining of a line with
     * @param withinBlock   If true we are saving bytes as we read
     * @return true         If we found the match, else <code>false</code>. EOF will return <code>false</code>
     * @throws java.io.IOException
     */
    private boolean readLinesBeforeStartsWithMatch(String matchString,
            boolean withinBlock) throws IOException {

        Text value;
        int newSize;

        if (this.pushBackString != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Wasn't expecting a saved line in 'readLinesBeforeStartsWithMatch'");
            }
            value = new Text(this.pushBackString);
            newSize = this.pushBackSize;
            this.pushBackSize = 0;
            this.pushBackString = null;
        } else {
            value = new Text();
            newSize = in.readLine(value, maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                    maxLineLength));
        }

        while (newSize != 0) {
            String line = value.toString();
            if (line.length() != 0 && line.startsWith(matchString)) {
                // keep this line and return true
                this.pushBackString = line;
                this.pushBackSize = newSize;  // silly business with new-lines
                return true;
            }

            this.pos += newSize;
            if (withinBlock) {
                buffer.write(line.getBytes());
                buffer.write("\n".getBytes());
            }

            newSize = in.readLine(value, this.maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE,
                    this.end - this.pos),
                    this.maxLineLength));
        }
        return false;
    }
}

