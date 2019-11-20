package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";
	private ConcurrentMap<Integer, Boolean> memory;


	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.memory = new ConcurrentHashMap<>();
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	/**
	 * Nachricht, die ein Worker vom Master bekommt um ein bestimmtes Charset abzusuchen.
	 */
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DoWorkMessage implements Serializable {
		private static final long serialVersionUID = 8940654955411400433L;
		private int id;
		private String hashedPassword;
		private List<char[]> charSets;
		private int passwordLength;
	}

	@Data
	@NoArgsConstructor
	public static class MoreWorkRequest implements Serializable {
		private static final long serialVersionUID = 8329058955803513344L;
	}


	/**
	 * Nachricht, die ein Worker an den Master sendet, wenn ein Hint aufgelöst werden konnte.
	 */
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class WorkFinishedMessage implements Serializable {
		private static final long serialVersionUID = 5400388160563796770L;
		private int id;
		private String solution;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class AbortWorkMessage implements Serializable {
		private static final long serialVersionUID = -2680862716297857008L;
		private int id;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(DoWorkMessage.class, this::handle)
				.match(AbortWorkMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(AbortWorkMessage message) {
		memory.put(message.id, false);
	}

	private void handle(DoWorkMessage message) {
		doActualWork(message);
		MoreWorkRequest req = new MoreWorkRequest();
		this.sender().tell(req, this.self());
	}

	private void doActualWork(DoWorkMessage message){

		List<char[]> charSets = message.getCharSets();
		if(!memory.getOrDefault(message.id, true)) return;//check first, might be useful if messages are received out of order
		memory.put(message.id, true);

		String hashedPassword = message.hashedPassword;
		byte[] hashedPasswordBytes = hexStringToByteArray(hashedPassword);
		for (char[] charSet : charSets) {
			if(!memory.get(message.id)) return; //exit if abort message received
			List<String> permutations = new ArrayList<>();
			generateAllPermutationsFromCharset(charSet, message.passwordLength, permutations, "");
			for(int i = 0; i < permutations.size(); i++){
				if(i % 100 == 0 && !memory.get(message.id)) return; //exit if abort message received

				byte[] crackHash = hashBytes(permutations.get(i));
				if(Arrays.equals(crackHash, hashedPasswordBytes)){//cracked
					WorkFinishedMessage finishedMsg = new WorkFinishedMessage();
					finishedMsg.id = message.id;
					finishedMsg.solution = permutations.get(i);
					this.sender().tell(finishedMsg, this.self());
					this.log().info("Found solution: " + permutations.get(i));
					return;
				}
			}
		}
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());

			this.getContext()
					.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
					.tell(new MoreWorkRequest(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	private byte[] hashBytes(String line) {//dont convert to string and strcmp, use byte[]s directly
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			return digest.digest(String.valueOf(line).getBytes("UTF-8"));
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	private void generateAllPermutationsFromCharset(char[] a, int size, List<String> l, String current) {
		for (int i = 0; i < a.length; i++) {
			String tmp = current + a[i];
			if(size == 1){
				l.add(tmp);
			}else{
				generateAllPermutationsFromCharset(a, size - 1, l, tmp);
			}

		}
	}


	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

	public static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
					+ Character.digit(s.charAt(i+1), 16));
		}
		return data;
	}
}