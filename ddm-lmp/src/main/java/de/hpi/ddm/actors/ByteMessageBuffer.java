package de.hpi.ddm.actors;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class ByteMessageBuffer {
    private static ByteMessageBuffer instance;
    private ConcurrentMap<Long, ConcurrentMap<Integer, byte[]>> storage;

    private ByteMessageBuffer() {
        this.storage = new ConcurrentHashMap<>();
    }

    /**
     * Singletons are bad. We know. Shared Singletons might be even worse. We know. Bacon!
     *
     * @return this
     */
    public static ByteMessageBuffer getBuffer() {
        if (instance == null) {
            instance = new ByteMessageBuffer();
        }
        return instance;
    }

    public Map<Integer, byte[]> getChunkMap(Long key) {
        return storage.get(key);
    }

    public void storeChunkOn(Long key, int offset, byte[] bytes) {
        storage.putIfAbsent(key, new ConcurrentHashMap<>());
        storage.get(key).put(offset, bytes);
    }

}
