package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import akka.serialization.Serializers;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

    private List<BytesMessage<Byte[]>> serialize(LargeMessage<?> message) {
        Serialization serialization = SerializationExtension.get(this.context().system());

        byte[] bytes = serialization.serialize(message).get();
        Serializer serializer = serialization.findSerializerFor(message);
        int serializerId = serializer.identifier();
        String manifest = Serializers.manifestFor(serializer, message);
        return null;
    }

    private LargeMessage<?> deserialize(List<BytesMessage<byte[]>> bytesMessages) {
        return null;
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context()
                .actorSelection(receiver.path().child(DEFAULT_NAME));
        byte[] msgBytes = KryoPoolSingleton.get().toBytesWithClass(message.getMessage());
        Long messageId = java.util.UUID.randomUUID().getLeastSignificantBits();
        for (int i = 0; i < msgBytes.length; i += MAX_BYTE_SIZE) {
            BytesMessage<byte[]> part = new BytesMessage<>();
            part.bytes = Arrays
                    .copyOfRange(msgBytes, i, Math.min(i + MAX_BYTE_SIZE, msgBytes.length));
            part.length = msgBytes.length;
            part.offset = i;
            part.receiver = receiver;
            part.sender = this.sender();
            part.messageId = messageId;
            receiverProxy.tell(part, this.self());
        }
    }

    private void handle(BytesMessage<?> message) {
        if (messageBuffer == null) {
            this.messageBuffer = new HashMap<>();
        }
        if (!messageBuffer.containsKey(message.messageId)) {
            messageBuffer.put(message.messageId, new HashMap<>());
        }
        Map<Integer, byte[]> chunkMap = messageBuffer.get(message.messageId);
        chunkMap.put(message.offset, (byte[]) message.bytes);
        if (chunkMap.size() == Math.ceil(message.length * 1.0 / MAX_BYTE_SIZE)) {
            byte[] finalMsg = new byte[message.length];
            for (Integer offset : chunkMap.keySet().stream().sorted()
                    .collect(Collectors.toList())) {
                System.arraycopy(chunkMap.get(offset), 0, finalMsg, offset,
                        chunkMap.get(offset).length);
            }
            Object origMsg =  KryoPoolSingleton.get().fromBytes(finalMsg);
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
