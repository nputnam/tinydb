package com.woot.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Entity {

    private final byte[] key;
    private final byte[] value;
    private final Long timestamp;
    private final boolean deleted;

    public Entity(byte[] key, byte[] value, Long timestamp, boolean deleted) {
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

    public Long getTimestamp() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entity entity = (Entity) o;

        if (deleted != entity.deleted) return false;
        if (!Arrays.equals(key, entity.key)) return false;
        if (!timestamp.equals(entity.timestamp)) return false;
        if (!Arrays.equals(value, entity.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + timestamp.hashCode();
        result = 31 * result + (deleted ? 1 : 0);
        return result;
    }
}
