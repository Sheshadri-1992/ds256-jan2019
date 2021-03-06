package jsr.jsk.prpe.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.erasurecoding.SampleEncoder;
import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.miscl.MyParser;
import jsr.jsk.prpe.thrift.CloseFileRequest;
import jsr.jsk.prpe.thrift.CloseFileResponse;
import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.MasterService;
import jsr.jsk.prpe.thrift.OpenFileRequest;
import jsr.jsk.prpe.thrift.OpenFileResponse;
import jsr.jsk.prpe.thrift.PutRequest;
import jsr.jsk.prpe.thrift.PutResponse;
import jsr.jsk.prpe.thrift.WriteBlockRequest;
import jsr.jsk.prpe.thrift.WriteBlockResponse;

public class PutRequestClass {
	
	
	class MyThread extends Thread {

		private EdgeService.Client edgeClient = null;
		private String outputFile = "";
		private byte[] shards = null;
		
		public MyThread(String s, EdgeService.Client myClient, String outputFile, byte[] shard) {
			super(s);
			
			edgeClient = myClient;
			this.outputFile = outputFile;
			shards = shard;
		}

		public void run() {
			System.out.println("Replication Running Thread: " + getName());
			
			PutRequest myPutReq = new PutRequest(outputFile, ByteBuffer.wrap(shards)); /**THIS was the EPIC MISTAKE **/
            try {
                PutResponse myRes = edgeClient.put(myPutReq);
                if (myRes.getResponse() == Constants.SUCCESS) {
                    LOGGER.info("Blocks successfully written ");                   
                }

            } catch (TException e) {

                e.printStackTrace();
            }
		}
	}/**End of multi threading class **/


	private String inputFileName= "";
	private String inputDirPath = "";
	private double storageBudget = 1.5;
	private int sessionHandle = 0;
	private FileOutputStream replicationStream;
	private FileOutputStream erasureCodingStream;
	private Integer NUM_ERASURE_CODING = 6;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PutRequestClass.class);
	
	/**
	 * Constructor
	 * @param argFileName
	 * @param argStorageBudget
	 */
	public PutRequestClass(String argFileName, double argStorageBudget) {
		inputFileName = argFileName;
		inputDirPath = Constants.DIRECTORY_PATH;
		storageBudget = argStorageBudget;
		
		MyParser parser = new MyParser();
		HashMap<String,String> codingMap = parser.returnErasureCoding();
		
		String num_coding = "6";
	
		if(codingMap!=null) {
			num_coding = codingMap.get("total");
			NUM_ERASURE_CODING = Integer.parseInt(num_coding);
			LOGGER.info("The number of erasure coding shards "+NUM_ERASURE_CODING);
		}
	}
	
	public void openRequest() {
		sessionHandle = openFileRequest();

		/** Create the logs directory to save the timings for erasure code and replication **/
		File myFile = new File(Constants.LOGS_DIR);
		if (myFile.exists() == false)
			myFile.mkdir();
		
		/** Create log streams **/
		try {
			replicationStream = new FileOutputStream(new File(Constants.LOGS_DIR + "rlogs_put.txt"),true);
			erasureCodingStream = new FileOutputStream(new File(Constants.LOGS_DIR + "elogs_put.txt"),true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * This is an important method, since sessionhandle is used for all future interactions
	 * @return The session handle is returned
	 */
	public int getSessionHandle() {
		return sessionHandle;
	}
	
	/** main method called from client driver which does put() **/
	public void putFile() {
		
		LOGGER.info("A request for put has come filename "+inputFileName);
		/** Session handle is set here **/
		writeFile();
	}
	
	
	/**
	 * 
	 * @return The session handle returned by the master
	 */
	private int openFileRequest() {
		int filesize = 0;
		File myFile = new File(inputDirPath+inputFileName);
		
		LOGGER.info("The input file path given is "+inputDirPath+inputFileName+ " : The storage budget is "+storageBudget);
		if(myFile.exists()) {
			filesize = (int)myFile.length();
		}
		
		MyParser parser = new MyParser();
		HashMap<String,String> masterLoc = parser.returnMasterLocation();
		
		String masterIp = "127.0.0.1";
		Integer masterPort = 8080;
		
		if(masterLoc!=null) {
			masterIp = masterLoc.get("ip");
			masterPort = Integer.parseInt(masterLoc.get("port"));
		}
		
				
		OpenFileRequest myRequest = new OpenFileRequest(); /** Openrequest has 4 required fields so I set them **/
		myRequest.setFilename(inputFileName);
		myRequest.setFilesize(filesize);
		myRequest.setStoragebudget(storageBudget);
		myRequest.setRequesttype(Constants.WRITE_REQUEST);
		
		TTransport transport = new TFramedTransport(new TSocket(masterIp, masterPort));
		try {
			transport.open();
		} catch (TTransportException e) {
			transport.close();
			LOGGER.error("Error opening connection to Master IP : {} and port : {}", masterIp, masterPort);
			e.printStackTrace();
		}
		
		TProtocol protocol = new TBinaryProtocol(transport);
		MasterService.Client masterClient = new MasterService.Client(protocol);
		LOGGER.info("OpenFile Request with master ");		
		
		OpenFileResponse myResponse = null;
		
		try {
			
			myResponse = masterClient.openFile(myRequest);
			int returnedHandle = myResponse.getHandle();
			LOGGER.info("The returned handle is "+returnedHandle);
			return returnedHandle;
			
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		
	}
	
	/**
	 * Takes file name and puts it to the data store
	 */
	private void writeFile() {
		
		int filesize = 0;
		File myFile = new File(inputDirPath+inputFileName);		
		filesize = (int)myFile.length(); /** This is the error which was giving problem **/
		
		
		double numBlocks = filesize / (1.0 *Constants.BLOCKSIZE);		
		double integerPart = (int) Math.floor(numBlocks);		
	
		int fullBlocks =0;
		fullBlocks = (int) integerPart;
		
		try {
			FileInputStream myFileInput = new FileInputStream(myFile);
			int index = 0;
			
			while(index<fullBlocks) {
				
				byte[] buffer = new byte[Constants.BLOCKSIZE];				
				myFileInput.read(buffer); /** File reading happens here **/
				
				writeBlocks(buffer);
				LOGGER.info("The bytes about to be written are "+buffer.length);
				index++; /** very important **/
			}/** End of while loop **/
			
			/** Last block of the file **/
			int remainingBytes = filesize - fullBlocks*Constants.BLOCKSIZE;			
			byte[] buffer = new byte[remainingBytes];			
			myFileInput.read(buffer);
						
			writeBlocks(buffer);
			myFileInput.close();			
			
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	/** Intermediate helper method 
	 *  It passes the session handle to the master node to get the datanode locations to write the blocks
	 * 
	 * **/
	private void writeBlocks(byte[] data) {
		
		MyParser parser = new MyParser();
		HashMap<String,String> masterLoc = parser.returnMasterLocation();
		
		String masterIp = "127.0.0.1";
		Integer masterPort = 8080;
		
		if(masterLoc!=null) {
			masterIp = masterLoc.get("ip");
			masterPort = Integer.parseInt(masterLoc.get("port"));
		}
		
		
		WriteBlockRequest myWriteBlockReq = new WriteBlockRequest(sessionHandle); /** Session handle is passed here **/
		
		TTransport transport = new TFramedTransport(new TSocket(masterIp, masterPort));
		try {
			transport.open();
		} catch (TTransportException e) {
			transport.close();
			LOGGER.error("Error opening connection to Master IP : {} and port : {}", masterIp, masterPort);
			e.printStackTrace();
		}
		
		TProtocol protocol = new TBinaryProtocol(transport);
		MasterService.Client masterClient = new MasterService.Client(protocol);
		LOGGER.info("WriteBlock Request with master ");
		
		WriteBlockResponse response = null;
		
		try {
			response = masterClient.requestBlocksToWrite(myWriteBlockReq);
			
			int type = response.getType();
			int blockNum = response.getBlocknum();
			ArrayList<DataNodeLocation> myDataLocs = (ArrayList<DataNodeLocation>) response.getLocations(); 
			
			LOGGER.info("The type is "+type+" blockNum is "+blockNum);
			for(DataNodeLocation myDataloc : myDataLocs) {
				LOGGER.info("The location is "+myDataloc.getNodeid()+" : "+myDataloc.getIp()+ " : "+myDataloc.getPort());
			}
					
			if(type==Constants.REPLICATION) {
				
				long start = System.currentTimeMillis();
				writeConcurrently(Constants.NUM_REPLICATION, Constants.REPLICATION,blockNum, data, myDataLocs);
				long end = System.currentTimeMillis();
				
				long timePerBlock = end - start;
				LOGGER.info("replication timing "+timePerBlock);
				String row = "r,"+blockNum+","+timePerBlock+"\n";
				replicationStream.write(row.getBytes());
				
			}else {
				
				long start = System.currentTimeMillis();
				writeConcurrently(NUM_ERASURE_CODING, Constants.ERASURE_CODING, blockNum, data, myDataLocs);
				long end = System.currentTimeMillis();
				
				long timePerBlock = end - start;
				LOGGER.info("erasure coding timing "+timePerBlock);
				String row = "e,"+blockNum+","+timePerBlock+"\n";
				erasureCodingStream.write(row.getBytes());
			}
			
		} catch (TException e) {
		
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/** ACtual writing happens here, either Erasure coding or replication **/
	public void writeConcurrently(int numThreads, int type,int blockNum ,byte[] data,ArrayList<DataNodeLocation> myDataLocs) {
		
		if(type==Constants.ERASURE_CODING) { /**ERASURE CODING **/
			SampleEncoder myEncoder = new SampleEncoder();
			try {
				LOGGER.info("Case of Erasure coding");
				myEncoder.encode(blockNum+"", myDataLocs, data);
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}else { /** REPLICATION **/
			LOGGER.info("Case of replication");
			
			MyThread threads[] = new MyThread[Constants.NUM_REPLICATION];
			int index = 0;
			
			for(DataNodeLocation myDataLoc : myDataLocs) {
				
				String IP = myDataLoc.getIp();
				int port = myDataLoc.getPort();
				
				TTransport transport = new TFramedTransport(new TSocket(IP, port));
	    		try {
	    			transport.open();
	    		} catch (TTransportException e) {
	    			transport.close();
	    			LOGGER.error("Error opening connection to Master IP : {} and port : {}", IP, port);
	    			e.printStackTrace();
	    			return;
	    		}
	    		
	    		TProtocol protocol = new TBinaryProtocol(transport);
	    		EdgeService.Client myClient = new EdgeService.Client(protocol);
	    		
	    		threads[index] = new MyThread(index+"", myClient, blockNum+"", data);
	    		threads[index].start();	    		
	    		
	    		index++; /**This is important **/
			}
			
			for(int i=0;i<Constants.NUM_REPLICATION;i++) {
				try {
					threads[i].join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}		
	}
	
	/** Close request to clear the session **/
	public void closeRequest() {
		MyParser parser = new MyParser();
		HashMap<String,String> masterLoc = parser.returnMasterLocation();
		
		String masterIp = "127.0.0.1";
		Integer masterPort = 8080;
		
		if(masterLoc!=null) {
			masterIp = masterLoc.get("ip");
			masterPort = Integer.parseInt(masterLoc.get("port"));
		}
				
		TTransport transport = new TFramedTransport(new TSocket(masterIp, masterPort));
		try {
			transport.open();
		} catch (TTransportException e) {
			transport.close();
			LOGGER.error("Error opening connection to Master IP : {} and port : {}", masterIp,
					masterPort);
			e.printStackTrace();
		}

		TProtocol protocol = new TBinaryProtocol(transport);
		MasterService.Client masterClient = new MasterService.Client(protocol);

		LOGGER.info("Close block Request with master ");
		try {
			CloseFileResponse response = masterClient.closeFile(new CloseFileRequest(sessionHandle));
			LOGGER.info("The response is " + response.getStatus());
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		/** close the file streams **/
		try {
			erasureCodingStream.close();
			replicationStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
}
