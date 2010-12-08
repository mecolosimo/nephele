/**
 * Created on October 15, 2009.
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
package org.mitre.nio;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Class that wraps a {@link ByteBuffer} in an {@link InputStream}.
 *
 * <pre>
 * FileInputStream fins = new FileInputStream(input);
   FileChannel channel = fins.getChannel();
 * MappedByteBuffer nBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) channel.size());  // if file > 2GB, only returns that part
 * nBuffer.clear();      // so that you start reading from the begining
 * DataInputStream dis = new DataInputStream( new BufferedInputStream( new ByteBufferBackedInputStream( (ByteBuffer) nBuffer)) );
 * </pre>
 *
 * @author Marc Colosimo
 */
public class ByteBufferBackedInputStream extends InputStream {

    ByteBuffer buf;

    ByteBufferBackedInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public int remaining() {
        return this.buf.remaining();
    }

    public ByteBufferBackedInputStream clear() {
        this.buf.clear();
        return this;
    }

    @Override
    public synchronized int read() throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get();
    }

    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }
}
