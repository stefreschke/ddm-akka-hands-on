package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Array;
import scala.Int;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";
	public class JobStatus{
		public String[] line;
		public boolean finished = false;
	}
	private static ConcurrentHashMap<Integer, JobStatus> jobStatusMap;

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		jobStatusMap = new ConcurrentHashMap<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(Worker.WorkFinishedMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	public List<char[]> generateCombinations(char[] chars, int r) {
		int n = chars.length;
		List<char[]> combinations = new ArrayList<>();
		char[] combination = new char[r];

		Map<Character, Integer> charToIndexMap = new HashMap<>();//required for (1) below as java has no indexOf
		for(int i = 0; i < n; i++){
			charToIndexMap.put(chars[i], i);
		}

		// initialize with lowest lexicographic combination
		System.arraycopy(chars, 0, combination, 0, r);

		while (combination[r - 1] < n) {
			combinations.add(combination.clone());

			// generate next combination in lexicographic order
			int t = r - 1;
			while (t != 0 && combination[t] == n - r + t) {
				t--;
			}
			combination[t]++;
			for (int i = t + 1; i < r; i++) {
				combination[i] = chars[charToIndexMap.get(combination[i - 1]) + 1];//(1)
			}
		}

		return combinations;
	}

	protected void handle(Worker.WorkFinishedMessage message) {
		int id = message.getId();
		String result = message.getSolution();
		//TODO: write result somewhere

		jobStatusMap.get(id).finished = true;
		Worker.AbortWorkMessage abort = new Worker.AbortWorkMessage();
		abort.setId(id);
		for (ActorRef k: this.workers) {
			if(!k.equals(this.sender())) k.tell(abort, this.self());
		}
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
		}
		
		for (String[] line : message.getLines()){
			int jobId = Integer.parseInt(line[0]);
			JobStatus status = new JobStatus();
			status.line = line;
			jobStatusMap.put(jobId, status);

			int hintCount = line.length - 5;
			int passwordCharCount = line[2].length() - hintCount;
			int passwordLength = Integer.parseInt(line[3]);
			char[] passwordCharset = line[2].toCharArray();

			//calc charset combinations
			List<char[]> combinations = generateCombinations(passwordCharset, passwordCharCount);
			int workPerNode = combinations.size() / this.workers.size();
			int curIndex = 0;
			for(int node = 0; node < this.workers.size(); node++){
				int end = curIndex + workPerNode;
				if(node == this.workers.size() - 1) end = combinations.size(); // last node gets all remaining (rounding errors!)
				Worker.DoWorkMessage msg = new Worker.DoWorkMessage();
				msg.setHashedPassword(line[4]);
				msg.setId(jobId);
				msg.setPasswordLength(passwordLength);
				msg.setCharSets(combinations.subList(curIndex, end));
				this.workers.get(node).tell(msg, this.self());
				curIndex = end;
			}

		}
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
