package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

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
	private static Queue<Worker.DoWorkMessage> workQueue;

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		jobStatusMap = new ConcurrentHashMap<>();
		workQueue = new ConcurrentLinkedQueue<>();
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
				.match(Worker.MoreWorkRequest.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	public List<char[]> generateCombinations(char[] chars, int r) {
		int n = chars.length;
		List<int[]> combinations = new ArrayList<>();
		int[] combination = new int[r];

		// initialize with lowest lexicographic combination
		for (int i = 0; i < r; i++) {
			combination[i] = i;
		}

		while (combination[r - 1] < n) {
			combinations.add(combination.clone());

			// generate next combination in lexicographic order
			int t = r - 1;
			while (t != 0 && combination[t] == n - r + t) {
				t--;
			}
			combination[t]++;
			for (int i = t + 1; i < r; i++) {
				combination[i] = combination[i - 1] + 1;
			}
		}

		List<char[]> combChars = new ArrayList<>();
		for (int[] intcomb : combinations) {
			char[] comb = new char[intcomb.length];
			for (int j = 0; j < intcomb.length; j++) {
				comb[j] = chars[intcomb[j]];
			}
			combChars.add(comb);
		}
		return combChars;
	}

	protected void handle(Worker.WorkFinishedMessage message) {
		int id = message.getId();
		String result = message.getSolution();

		//save/collect result
		String origLine[] = jobStatusMap.get(id).line;
		origLine[4] = result;
		String crackedLine = String.join(";", origLine);
		this.collector.tell(new Collector.CollectMessage(crackedLine), this.self());

		//inform other workers about finished job
		jobStatusMap.get(id).finished = true;
		Worker.AbortWorkMessage abort = new Worker.AbortWorkMessage();
		abort.setId(id);
		for (ActorRef k: this.workers) {
			if(!k.equals(this.sender())) k.tell(abort, this.self());
		}

		//terminate process if all passwords are cracked
		boolean unfinishedWork = false;
		for (Map.Entry<Integer, JobStatus> entry : jobStatusMap.entrySet()) {
			if (!entry.getValue().finished) {
				unfinishedWork = true;
				break;
			}
		}
		if(!unfinishedWork){
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.log().info("All cracked! Gracefully shutting down now!");
			this.log().info("Please do not count time after this message as work time!");
			try {
				Thread.sleep(1500); //wait for other actors (e.g. collector) to gracefully exit
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			this.terminate();
		}
	}

	protected void handle(Worker.MoreWorkRequest message) {
		Worker.DoWorkMessage msg = workQueue.poll();
		if (msg != null) {
			this.sender().tell(msg, this.self());
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
			this.distributeWorkInitial();
			this.terminateReader();
		}
		
		for (String[] line : message.getLines()){
			int jobId = Integer.parseInt(line[0]);
			JobStatus status = new JobStatus();
			status.line = line;
			jobStatusMap.put(jobId, status);

			int hintCount = line.length - 5;
			int passwordLength = Integer.parseInt(line[3]);
			char[] passwordCharset = line[2].toCharArray();
			int passwordCharCount = passwordCharset.length - hintCount;
			//calc charset combinations

			List<char[]> combinations = generateCombinations(passwordCharset, passwordCharCount);
			int workerSize = Math.max(workers.size(), 32);
			int workPerNode = Math.min(combinations.size() / workerSize / 2, 1);
			int curIndex = 0;
			for(int node = 0; node < workerSize; node++){
				int end = curIndex + workPerNode;
				if(node == workerSize - 1) end = combinations.size();
				Worker.DoWorkMessage msg = new Worker.DoWorkMessage();
				msg.setHashedPassword(line[4]);
				msg.setId(jobId);
				msg.setPasswordLength(passwordLength);
				msg.setCharSets(combinations.subList(curIndex, end));
				workQueue.add(msg);
				curIndex = end;
			}

		}
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void terminateReader(){
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void distributeWorkInitial(){
		for(ActorRef worker: this.workers){
			if(!workQueue.isEmpty()){
				Worker.DoWorkMessage work = workQueue.poll();
				if(work != null){
					worker.tell(work, this.self());
				}
			}
		}
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
