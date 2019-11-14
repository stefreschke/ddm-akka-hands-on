package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LargeMessageProxy extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "largeMessageProxy";
    public static final int MAX_BYTE_SIZE = 8192; //max 8kb
    private Map<Long, Map<Integer, byte[]>> messageBuffer;

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(LargeMessage.class, this::handle)
                .match(BytesMessage.class, this::handle).matchAny(object -> this.log()
                        .info("Received unknown message: \"{}\"", object.toString())).build();
    }

    private BytesMessage<byte[]> buildChunk(ActorRef receiver, byte[] bytes, int offset, long messageId, int totalLength) {
        BytesMessage<byte[]> byteMessage = new BytesMessage<>();
        byteMessage.bytes = bytes;
        byteMessage.length = totalLength;
        byteMessage.offset = offset;
        byteMessage.receiver = receiver;
        byteMessage.sender = this.sender();
        byteMessage.messageId = messageId;
        return byteMessage;
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context()
                .actorSelection(receiver.path().child(DEFAULT_NAME));
        byte[] allBytes = KryoPoolSingleton.get().toBytesWithClass(message.getMessage());
        long messageId = java.util.UUID.randomUUID().getLeastSignificantBits();
        for (int i = 0; i < allBytes.length; i += MAX_BYTE_SIZE) {
            receiverProxy.tell(buildChunk(receiver, Arrays
                    .copyOfRange(allBytes, i, Math.min(i + MAX_BYTE_SIZE, allBytes.length)), i, messageId, allBytes.length), this.self());
        }
    }

    private void setupMessageBufferForMessageId(long messageId) {
        if (messageBuffer == null) {
            this.messageBuffer = new HashMap<>();
        }
        if (!messageBuffer.containsKey(messageId)) {
            messageBuffer.put(messageId, new HashMap<>());
        }
    }

    private Object decodeChunks(int totalLength, Map<Integer, byte[]> chunkMap){
            byte[] finalMsg = new byte[totalLength];
            for (Integer offset : chunkMap.keySet().stream().sorted()
                    .collect(Collectors.toList())) {
                System.arraycopy(chunkMap.get(offset), 0, finalMsg, offset,
                        chunkMap.get(offset).length);
            }
            Object origMsg = KryoPoolSingleton.get().fromBytes(finalMsg);
            return origMsg;
    }

    private boolean isDoneDeserializing(int chunkCount, int totalLength) {
        return chunkCount == Math.ceil(totalLength * 1.0 / MAX_BYTE_SIZE);
    }

    private void handle(BytesMessage<?> message) {
        setupMessageBufferForMessageId(message.messageId);
        Map<Integer, byte[]> chunkMap = messageBuffer.get(message.messageId);
        chunkMap.put(message.offset, (byte[]) message.bytes);

        if (isDoneDeserializing(chunkMap.size(), message.length)) {
            Object origMsg = decodeChunks(message.length, chunkMap);
            message.getReceiver().tell(origMsg, message.sender);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage<T> implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        public int length;
        public int offset;
        public Long messageId;
        private T bytes;
        private ActorRef sender;
        private ActorRef receiver;
    }
}
