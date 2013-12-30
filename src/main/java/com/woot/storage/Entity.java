package com.woot.storage;

import java.nio.ByteBuffer;

public class Entity {

    private final byte[] key;
    private final byte[] value;
    private final long timestamp;
    private final boolean deleted;

    public Entity(byte[] key, byte[] value, long timestamp, boolean deleted) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.deleted = deleted;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public ByteBuffer toBytes() {
        ByteBuffer allocate = ByteBuffer.allocate(2 + 8 + 1 + 4 + key.length + value.length);
        allocate.putShort((short) key.length);
        allocate.putLong(timestamp);
        allocate.put(deleted ? Byte.MAX_VALUE : Byte.MIN_VALUE);
        allocate.putInt(value.length);
        allocate.put(key);
        allocate.put(value);
        return allocate;
    }
}
