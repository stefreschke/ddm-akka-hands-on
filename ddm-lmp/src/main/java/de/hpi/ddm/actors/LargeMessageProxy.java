package de.hpi.ddm.actors;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import it.unimi.dsi.fastutil.Hash;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
		public int length;
		public int offset;
		public Long messageId;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}
	public static final int MAX_BYTE_SIZE = 8192; //max 8kb

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(message);
			out.flush();
			byte[] msgBytes = bos.toByteArray();
			Long messageId = java.util.UUID.randomUUID().node();
			for(int i = 0; i < msgBytes.length; i += MAX_BYTE_SIZE){
				BytesMessage<byte[]> part = new BytesMessage<>();
				part.bytes = Arrays.copyOfRange(msgBytes, i, Math.min(i + MAX_BYTE_SIZE, msgBytes.length));
				part.length = msgBytes.length;
				part.offset = i;
				part.receiver = receiver;
				part.sender = this.sender();
				part.messageId = messageId;
				receiverProxy.tell(part, this.self());
			}
		} catch (IOException e) {
			this.log().error("Failed to serialize {}", e.getMessage());
		} finally {
			try {
				bos.close();
			} catch (IOException ex) {
			}
		}
	}

	private Map<Long, Map<Integer, byte[]>> messageBuffer;

	private void handle(BytesMessage<?> message) {
		if (messageBuffer == null) {
			this.messageBuffer = new HashMap<>();
		}
		if(!messageBuffer.containsKey(message.messageId)){
			messageBuffer.put(message.messageId, new HashMap<>());
		}
		Map<Integer, byte[]> chunkMap =  messageBuffer.get(message.messageId);
		chunkMap.put(message.offset, (byte[])message.bytes);
		if(chunkMap.size() == Math.ceil(message.length * 1.0 / MAX_BYTE_SIZE)){
			byte[] finalMsg = new byte[message.length];
			for (Integer offset :  chunkMap.keySet().stream().sorted().collect(Collectors.toList())) {
				System.arraycopy(chunkMap.get(offset), 0, finalMsg, offset, chunkMap.get(offset).length);
			}
			ByteArrayInputStream inputStream = new ByteArrayInputStream(finalMsg);
			try (ObjectInputStream objectInputStream =
						new ObjectInputStream(inputStream)){
				LargeMessage<?> origMsg = (LargeMessage<?>) objectInputStream.readObject();
				origMsg.getReceiver().tell(origMsg.message, origMsg.getSender());
			} catch (Exception e) {
				this.log().error("Failed to deserialize {}", e.getMessage());
			}
		}

	}
}
