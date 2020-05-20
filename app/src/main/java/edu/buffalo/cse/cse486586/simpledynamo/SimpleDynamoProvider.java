package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private Uri newUri;
	static String myPort = "";
	private String node_id;
	static final int SERVER_PORT = 10000;
	static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
	static final String[] EMULATORS = {"5554", "5556", "5558", "5560", "5562"};
	private final Lock lock = new ReentrantLock();
	Map<String, String> nodes = new TreeMap<String, String>();
	List<String> hashed = new ArrayList<String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.contains("@")|| selection.contains("*"))
		{
			File[] files = getContext().getFilesDir().listFiles();
			for(File file : files)
			{
				file.delete();
			}
		}
		else
		{
		for(String port : REMOTE_PORTS)
			{
					Socket socket = null;
					String message = "DELETE" + "--" + "";
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
						socket.setSoTimeout(1000);
						DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
						DataInputStream inputStream = new DataInputStream(socket.getInputStream());
						outputStream.writeUTF(message);
						outputStream.flush();
						Log.e(TAG, "SENT DELETE MESSAGE");
						String response = "";
						try {
							response = inputStream.readUTF();
						} catch (Exception e) {
							Log.e(TAG, "FAILED AVD");
						}
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
			File[] files = getContext().getFilesDir().listFiles();
			for(File file : files)
			{
				file.delete();
			}
		}
		return 0;
	}

	public void deletehandler()
	{
		File[] files = getContext().getFilesDir().listFiles();
		for(File file : files)
		{
			file.delete();
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		synchronized (uri){
			String key = (String)values.get("key");
			String value = (String) values.get("value");
			String hashed_key = "";
			try
			{
				hashed_key = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "Error in hashing insert key");
			}

			int owner_index = findPartition(hashed_key);
			String owneravdhash = hashed.get(owner_index);
			String owneravd = nodes.get(owneravdhash);
			String ownerPort = String.valueOf(Integer.parseInt(owneravd)*2);
			Log.e(TAG, "OWNER PORT =" + ownerPort);
			value = value + "::" + ownerPort;
			if(ownerPort.equals(myPort))
			{
				insertHandler(key, value);
			}
			else
			{
				Log.e(TAG, "Sending insert message to owner port");
				Socket socket = null;
				String message = "INSERT" + "--" + key + "--" + value;
				try
				{
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ownerPort));
					DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
					DataInputStream inputStream = new DataInputStream(socket.getInputStream());
					outputStream.writeUTF(message);
					outputStream.flush();
					Log.e(TAG, "Message sent to owner");
					String response = null;
					response = inputStream.readUTF();
					if(response == null)
					{
						Log.e(TAG, "FAILED AVD");
						socket.close();
					}
					else
					{
						socket.close();
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			replicate(owner_index, key, value);
		}
		return null;
	}

	public void insertHandler(String key, String value)
	{
		try
		{
			FileOutputStream fileOutput = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
			fileOutput.write(value.getBytes());
			fileOutput.close();
			Log.e(TAG, "INSERTED VALUE");
			Log.e(TAG, key + "--" + value);
		} catch (Exception e)
		{
			Log.e(TAG, "Error in writing file");
		}
	}

	public void replicate(int index, String key, String value)
	{
		Log.e(TAG, "IN REPLICATE");
		int owner_index = index;
		int rep1 = 0, rep2 = 0;
		for(int i=0;i<hashed.size();i++)
		{
			if(i == owner_index && owner_index == (hashed.size() - 2))
			{
				rep1 = hashed.size()-1;
				rep2 = 0;
			}
			else if(i == owner_index && owner_index == (hashed.size() - 1))
			{
				rep1 = 0;
				rep2 = 1;
			}
			else if(i == owner_index)
			{
				rep1 = i+1;
				rep2 = i+2;
			}
		}
		Log.e(TAG, "REPLICAS ARE" + rep1 + "--" +rep2);
		int[] replicas = new int[] {rep1, rep2};
		for(int replica : replicas)
		{
			String replica_hash = hashed.get(replica);
			String avd = nodes.get(replica_hash);
			String port = String.valueOf(Integer.parseInt(avd) * 2);
			if(port.equals(myPort))
			{
				insertHandler(key, value);
			}
			else {
				String message = "REPLICA" + "--" + port + "--" + key + "--" + value;
				Log.e(TAG, message);
				Socket socket = null;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
					DataInputStream inputStream = new DataInputStream((socket.getInputStream()));
					outputStream.writeUTF(message);
					outputStream.flush();
					Log.e(TAG, "REPLICA MESSAGE SENT TO PORT STORING REPLICA");
					String response = inputStream.readUTF();
					if (response == null) {
						Log.e(TAG, "FAILED AVD");
						socket.close();
					} else {
						Log.e(TAG, response);
						socket.close();
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public int findPartition(String hashed_key)
	{
		for(int i=0;i<hashed.size();i++)
		{
			int val = hashed_key.compareTo(hashed.get(i));
			if(i == 0)
			{
				int val1 = hashed_key.compareTo(hashed.get(hashed.size()-1));
				if(val <= 0 || val1 > 0)
				{
					return i;
				}
			}
			else
			{
				int val1 = hashed_key.compareTo(hashed.get(i-1));
				if(val <= 0 && val1 > 0)
				{
					return i;
				}
			}
		}
		return 0;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		lock.lock();
		newUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		node_id = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(node_id) * 2));
		Log.e(TAG, "Emulator port: " + myPort);

		try
		{
			for(String avd : EMULATORS)
			{
				String avdHash = genHash(avd);
				nodes.put(avdHash, avd);
				hashed.add(avdHash);
			}
		} catch (NoSuchAlgorithmException e)
		{
			Log.e(TAG, "Error in hashing emulator numbers");
		}

		try
		{
			ServerSocket sever = new ServerSocket();
			sever.setReuseAddress(true);
			sever.bind(new InetSocketAddress(SERVER_PORT));
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, sever);
			Log.e(TAG, "SERVER TASK CALLED");
		} catch (IOException e) {
			Log.e(TAG, "Exception in creating Server Socket");
			e.printStackTrace();
		}

		Collections.sort(hashed);

		for(String key : hashed)
		{
			Log.e(TAG, "KEY FROM HASHED" + key);
		}

		String message = "RECOVERY";
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, myPort);
		Log.e(TAG, "Client Task called");

		for(String hash : hashed)
		{
			Log.e(TAG, hash + "---");
		}

		for(String x : nodes.keySet())
		{
			Log.e(TAG, x + "-----" + nodes.get(x));
		}

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		//REFERENCE TAKEN FROM PA3
		synchronized (uri) {
			String[] cols = new String[]{"key", "value"};
			MatrixCursor cursor = new MatrixCursor(cols);
			if (selection.equals("@")) {
				Log.e(TAG, "DUMP ALL FROM MYSELF");
				File[] files = getContext().getFilesDir().listFiles();
				for (File file : files) {
					try {
						String key = file.getName();
						Log.e(TAG, key + "filename");
						FileInputStream fileInputStream = new FileInputStream(file);
						BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
						String line = reader.readLine();
						reader.close();
						fileInputStream.close();
						String[] lines = line.split("::");
						Log.e(TAG, lines[0] + "||" + lines[1]);
						String newRow[] = new String[]{key, lines[0]};
						cursor.addRow(newRow);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				return cursor;
			} else if (selection.equals("*")) {
				Log.e(TAG, "DUMPALL QUERY");
				File[] files = getContext().getFilesDir().listFiles();
				for (File file : files) {
					try {
						String key = file.getName();
						FileInputStream fileInputStream = new FileInputStream(file);
						BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
						String line = reader.readLine();
						reader.close();
						fileInputStream.close();
						String[] lines = line.split("::");
						String newRow[] = new String[]{key, lines[0]};
						cursor.addRow(newRow);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				for (String remotePort : REMOTE_PORTS) {
					if (!remotePort.equals(myPort)) {
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
							String message = "DUMPALL" + "--" + myPort;
							DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
							outputStream.writeUTF(message);
							outputStream.flush();
							DataInputStream inputStream = new DataInputStream(socket.getInputStream());
							String response = inputStream.readUTF();
							if (response != ":") {
								String[] keys_values = response.split(":");
								if (keys_values.length == 2) {
									String[] keys = keys_values[0].split("-");
									String[] values = keys_values[1].split("-");
									for (int i = 0; i < keys.length; i++) {
										String[] newRow = new String[]{keys[i], values[i]};
										cursor.addRow(newRow);
									}
								}
							}
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				return cursor;
			} else {
				//ADD CODE FOR SINGLE QUERY
				Log.e(TAG, "IN SINGLE QUERY");
				String queryHash = "";
				try {
					queryHash = genHash(selection);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				int ownerIndex = findPartition(queryHash);
				String owneravdhash = hashed.get(ownerIndex);
				String owneravd = nodes.get(owneravdhash);
				String ownerPort = String.valueOf((Integer.parseInt(owneravd) * 2));

				if (ownerPort.equals(myPort)) {
					Log.e(TAG, "I HAVE IT");
					try {
						File fileInput = new File(getContext().getFilesDir().getAbsolutePath() + File.separator + selection);
						FileInputStream fileInputStream = new FileInputStream(fileInput);
						BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
						String line = reader.readLine();
						reader.close();
						String[] lines = line.split("::");
						String newRow[] = new String[]{selection, lines[0]};
						cursor.addRow(newRow);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return cursor;
				} else {
					try {
						Log.e(TAG, "SENDING TO OWNER" + ownerPort);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ownerPort));
						DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
						DataInputStream inputStream = new DataInputStream(socket.getInputStream());
						String message = "DUMPONE" + "--" + selection;
						outputStream.writeUTF(message);
						outputStream.flush();
						String response = "";
						try {
							response = inputStream.readUTF();
						} catch (Exception e)
						{
							Log.e(TAG, "FAILED AVD");
							response = "nope";
						}
						if(response.equals("nope")) {
							int rep1 = 0, rep2 = 0;
							for (int i = 0; i < hashed.size(); i++) {
								if (i == ownerIndex && ownerIndex == (hashed.size() - 2)) {
									rep1 = hashed.size() - 1;
									rep2 = 0;
								} else if (i == ownerIndex && ownerIndex == (hashed.size() - 1)) {
									rep1 = 0;
									rep2 = 1;
								} else if (i == ownerIndex) {
									rep1 = i + 1;
									rep2 = i + 2;
								}
							}

							String rep1hash = hashed.get(rep1);
							String rep1avd = nodes.get(rep1hash);
							String rep1Port = String.valueOf((Integer.parseInt(rep1avd) * 2));

							String rep2hash = hashed.get(rep2);
							String rep2avd = nodes.get(rep2hash);
							String rep2Port = String.valueOf((Integer.parseInt(rep2avd) * 2));
							String reply1 = "", reply2 = "";
							try {
								Log.e(TAG, "QUERYING REPLICA 1" + rep1Port);
								Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(rep1Port));
								DataOutputStream outputStream1 = new DataOutputStream(socket1.getOutputStream());
								DataInputStream inputStream1 = new DataInputStream(socket1.getInputStream());
								String message1 = "DUMPONE" + "--" + selection;
								outputStream1.writeUTF(message1);
								outputStream1.flush();
								reply1 = inputStream1.readUTF();


								Log.e(TAG, "QUERYING REPLICA 2" + rep2Port);
								Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(rep2Port));
								DataOutputStream outputStream2 = new DataOutputStream(socket2.getOutputStream());
								DataInputStream inputStream2 = new DataInputStream(socket2.getInputStream());
								String message2 = "DUMPONE" + "--" + selection;
								outputStream2.writeUTF(message2);
								outputStream2.flush();
								reply2 = inputStream2.readUTF();

							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}

							if (reply1.equals(""))
								response = reply2;
							else
								response = reply1;
						}
						String key_value[] = response.split(":");
						String newRow[] = new String[]{selection, key_value[1]};
						cursor.addRow(newRow);
						return cursor;
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				return null;
			}
		}
	}

	public String queryHandler(String selection) {
		//REFERENCE TAKEN FROM PA3
		String key = "";
		String value = "";
		if (selection.equals("*") || selection.equals("@")) {
			File[] files = getContext().getFilesDir().listFiles();
			if(files.length == 0)
			{
				return "";
			}
			else {
				for (File file : files) {
					try {
						Log.e(TAG, "Looping through files");
						String name = file.getName();
						Log.e(TAG, name);
						FileInputStream fileInputStream = new FileInputStream(file);
						BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
						String line = reader.readLine();
						reader.close();
						fileInputStream.close();
						String[] lines = line.split("::");
						key = key + "-" + name;
						Log.e(TAG, key);
						value = value + "-" + lines[0];
						Log.e(TAG, value);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (key.length() > 0) {
					key = key.substring(1);
					value = value.substring(1);
				}
				String reply = key + ":" + value;
				return reply;
			}
		} else {
			String reply = "";
			try {
				File fileInput = new File(getContext().getFilesDir().getAbsolutePath() + File.separator + selection);
				FileInputStream fileInputStream = new FileInputStream(fileInput);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
				String line = reader.readLine();
				reader.close();
				fileInputStream.close();
				String[] lines = line.split("::");
				reply = selection + ":" + lines[0];
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return reply;
		}
	}

	public String recoveryResponse(String ownerPort) {
		Log.e(TAG, "RECOVERY REQUEST FROM" + ownerPort);
		String key = "";
		String value = "";
		File[] files = getContext().getFilesDir().listFiles();
		if(files.length == 0)
		{
			return "EMPTY";
		}
		for (File file : files) {
			try {
				String name = file.getName();
				FileInputStream fileInputStream = new FileInputStream(file);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
				String line = reader.readLine();

					key = key + "-" + name;
					value = value + "-" + line;

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (key.length() > 0) {
			key = key.substring(1);
			value = value.substring(1);
		}
		else{
			return "EMPTY";
		}

		String response = key + "!!" + value;
		Log.e(TAG, "RETURNED RESPONSE FROM RECOVERYRESPONSE");
		return response;
	}


			@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ClientTask extends AsyncTask<String , Void, Void>
	{

		@Override
		protected Void doInBackground(String... strings) {
			Log.e(TAG, "RECOVERY TASK");
			String hash = "";
			try
			{
				hash = genHash(node_id);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			int orig = 0, rep1 = 0, rep2 = 0;

			String[] check = nodes.keySet().toArray(new String[nodes.size()]);

			for(String key : check)
			{
				Log.e(TAG, "RETREIVED KEYSET:" + key);
			}

			for(int i=0;i<check.length;i++)
			{
				if(check[i].equals(hash))
				{
					orig = i;
					break;
				}
			}
			Log.e(TAG, "MY INDEX IS:" + orig);
			for(int i=0;i<check.length;i++)
			{
				if(i == orig && orig == (check.length - 1))
				{
					rep1 = check.length-2;
					rep2 = 0;
				}
				else if(i == orig && orig == 0)
				{
					rep1 = 1;
					rep2 = check.length-1;
				}
				else if(i == orig)
				{
					rep1 = i+1;
					rep2 = i-1;
				}
			}
			int[] replicas =  new int[] {rep1, rep2};
			for(int replica : replicas)
			{
				String replica_hash = check[replica];
				String avd = nodes.get(replica_hash);
				String port = String.valueOf(Integer.parseInt(avd) * 2);
				String message = "RECOVERY" + "--" + myPort;
				try
				{
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
					DataInputStream inputStream = new DataInputStream((socket.getInputStream()));
					socket.setSoTimeout(100);
					outputStream.writeUTF(message);
					outputStream.flush();
					Log.e(TAG, "SENT RECOVERY REQUEST TO" + port);
					String response = null;
					response = inputStream.readUTF();
					if(response.equals("EMPTY") || response == null || response.equals(":"))
						continue;
					else {
						Log.e(TAG, "RECEIVED RECOVERY RESPONSE IS" + response);
						String[] keys_values = response.split("!!");
						String[] keys = keys_values[0].split("-");
						if (keys.length > 0) {
							String[] values = keys_values[1].split("-");
							for (int i = 0; i < keys.length; i++) {
								Log.e(TAG, keys[i]);
								String queryHash = "";
								try {
									queryHash = genHash(keys[i]);
								} catch (NoSuchAlgorithmException e) {
									e.printStackTrace();
								}
								int ownerIndex = findPartition(queryHash);
								String owneravdhash = hashed.get(ownerIndex);
								String owneravd = nodes.get(owneravdhash);
								String ownerPort = String.valueOf((Integer.parseInt(owneravd) * 2));
								Log.e(TAG, "GOT OWNER PORT");
								int rep1_1 = 0, rep1_2 = 0;
								for(int j=0;j<hashed.size();j++)
								{
									if(hashed.get(j) == owneravdhash && j == (hashed.size() - 2))
									{
										rep1_1 = hashed.size()-1;
										rep1_2 = 0;
									}
									else if(hashed.get(j) == owneravdhash && j == (hashed.size() - 1))
									{
										rep1_1 = 0;
										rep1_2 = 1;
									}
									else if(hashed.get(j) == owneravdhash)
									{
										rep1_1 = j+1;
										rep1_2 = j+2;
									}
								}
								Log.e(TAG, "GOT REPLICA PORTS");
								String replica1_hash = hashed.get(rep1_1);
								String avd1 = nodes.get(replica1_hash);
								String port1 = String.valueOf(Integer.parseInt(avd1) * 2);

								String replica2_hash = hashed.get(rep1_2);
								String avd2 = nodes.get(replica2_hash);
								String port2 = String.valueOf(Integer.parseInt(avd2) * 2);
								Log.e(TAG, "ONLY INSERTING IF: " + ownerPort + port1 + port2);
								if(myPort.equals(ownerPort) || myPort.equals(port1) || myPort.equals(port2)) {
									insertHandler(keys[i], values[i]);
									Log.e(TAG, values[i]);
								}
							}
						}
						socket.close();
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return null;
		}

		@Override
		protected void onPostExecute(Void aVoid)
		{
			super.onPostExecute(aVoid);
			lock.unlock();
		}
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			//CODE TAKEN FROM PA3
			Log.e(TAG, "SERVER STARTED");
			ServerSocket serverSocket = serverSockets[0];
			while (true) {
				try {
					Socket server = serverSocket.accept();
					lock.lock();
					lock.unlock();
					DataInputStream inputStream = new DataInputStream(server.getInputStream());
					DataOutputStream outputStream = new DataOutputStream(server.getOutputStream());
					String message = inputStream.readUTF();
					String[] message_splits = message.split("--");
					if (message_splits[0].equals("INSERT")) {
						String key = message_splits[1];
						String value = message_splits[2];
						insertHandler(key, value);
						outputStream.writeUTF("ACK");
						outputStream.flush();
					} else if (message_splits[0].equals("REPLICA")) {
						Log.e(TAG, "REPLICA RECEIVED");
						if (message_splits[1].equals(myPort)) {
							String key = message_splits[2];
							String value = message_splits[3];
							insertHandler(key, value);
							outputStream.writeUTF("ACK");
							outputStream.flush();
						}
					} else if(message_splits[0].equals("RECOVERY"))
					{
						String toSend = recoveryResponse(message_splits[1]);
						outputStream.writeUTF(toSend);
						outputStream.flush();
						Log.e(TAG, "SENT RECOVERY RESPONSE BACK TO PORT RECOVERING" + toSend);
						server.close();
					}
					else if(message_splits[0].equals("DUMPALL"))
					{
						String toSend = queryHandler("*");
						outputStream.writeUTF(toSend);
						outputStream.flush();
						server.close();
					}
					else if(message_splits[0].equals("DUMPONE"))
					{
						String toSend = queryHandler(message_splits[1]);
						outputStream.writeUTF(toSend);
						outputStream.flush();
						server.close();
					}
					else if(message_splits[0].equals("DELETE"))
					{
						Log.e(TAG, "DELETE REQUEST RECEIVED");
						deletehandler();
						outputStream.writeUTF("ACK");
						outputStream.flush();
						server.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
}
