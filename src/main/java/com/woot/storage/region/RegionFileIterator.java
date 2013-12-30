package com.woot.storage.region;

import com.google.common.collect.AbstractIterator;
import com.woot.storage.Entity;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/*
    Class to iterate through the values of a region file. A region file has the format of

    short <key length>
    long timestamp
    byte deleted or not
    int <value length>
    bytes <key bytes>
    byte <value length>
 */
public class RegionFileIterator extends AbstractIterator<Entity> {

    private final FileChannel inChannel;

    public RegionFileIterator(File file) {
        try {
            RandomAccessFile aFile = new RandomAccessFile(file, "r");
            inChannel = aFile.getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error opening file ", e);
        }
    }

    @Override
    protected Entity computeNext() {

        try {
            ByteBuffer preamble = ByteBuffer.allocate(Short.SIZE / 8 + Long.SIZE / 8 + 1 + Integer.SIZE / 8);
            int read = inChannel.read(preamble);
            // No more elements return a null
            if (read <= 0) {
                inChannel.close();
                return endOfData();
            }
            preamble.rewind();

            short keyLength = preamble.getShort();
            long timestamp = preamble.getLong();
            byte deleted = preamble.get();
            int valueLength = preamble.getInt();

            ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
            inChannel.read(keyBuffer);

            ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
            inChannel.read(valueBuffer);

            Entity entity = new Entity(keyBuffer.array(), valueBuffer.array(), timestamp, deleted == Byte.MAX_VALUE ? Boolean.TRUE : Boolean.FALSE);

            if (entity.isDeleted()) {
                return null;
            }
            return entity;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
