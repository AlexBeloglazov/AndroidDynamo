package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

public class SimpleDynamoProvider extends ContentProvider {

    private static final String TAG = "*DYNAMO*";
    private static final String SPLITTER = "%SPLIT%";
    private static final String SEPARATOR = "%SEP%";
    private static final String DELETE = "%DELETE%";
    private static final String DELETEALL = "%DELETEALL%";
    private static final String INSERT = "%INSERT%";
    private static final String QUERY = "%QUERY%";
    private static final String RECOVERDB = "%RECOVERDB%";
    private static final String RECOVERREPLICA = "%RECOVERREPLICA%";

    private final int NUMBER_OF_REPLICAS = 2;
    private final int serverPort = 10000;
    private final String[] ports = {"5554", "5556", "5558", "5560", "5562"};
    private final int numberOfNodes = ports.length;
    private TreeSet<String> lookUp = new TreeSet<>();
    private ArrayList<String> indexing = null;
    private HashMap<String, String> hash2port = new HashMap<>();
    private String me = null, meHash = null;

    private SQLiteDatabase mDB = null;


    protected static final class DBOpener extends SQLiteOpenHelper {

        public static final String DB_NAME = "Dynamo.db";
        public static final int DB_VERSION = 1;
        public static final String MAIN_TABLE = "dynamo";
        public static final String[] REPLICAS = {"replica0", "replica1"};
        public static final String COLUMN_NAME_KEY = "key";
        public static final String COLUMN_NAME_MESSAGE = "value";

        DBOpener(Context context) {
            super(context, DB_NAME, null, DB_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("CREATE TABLE "+MAIN_TABLE+" ("+COLUMN_NAME_KEY+" TEXT PRIMARY KEY, " +
                    COLUMN_NAME_MESSAGE+" TEXT)");

            for(String replica: REPLICAS) {
                db.execSQL("CREATE TABLE "+replica+" ("+COLUMN_NAME_KEY+" TEXT PRIMARY KEY, " +
                        COLUMN_NAME_MESSAGE+" TEXT)");
            }
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS"+MAIN_TABLE);
            onCreate(db);
        }
    }


    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

        Log.i("*DELETE*", "deleting key="+selection);
        String targetNode = getSuccessor(SHA1.genHash(selection));

        switch (selection) {
            case "*":
                for(String nodeId: ports) {
                    try {
                        sendReceive(nodeId, DELETEALL + SEPARATOR + meHash, false);
                    }
                    catch (Exception e) {}
                }

            case "@":
                targetNode = meHash;

            default:
                for(String nodeId: readMap(targetNode)) {
                    try {
                        sendReceive(nodeId, DELETE + SEPARATOR + selection + SEPARATOR + targetNode, false);
                    }
                    catch (Exception e) {}
                }
        }

		return 0;
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString(DBOpener.COLUMN_NAME_KEY);
        String value = values.getAsString(DBOpener.COLUMN_NAME_MESSAGE);
        String targetNode = getSuccessor(SHA1.genHash(key));
        for(String nodeId: writeMap(targetNode)) {
            try {
                sendReceive(nodeId, INSERT + SEPARATOR + key + SPLITTER + value + SEPARATOR + targetNode, false);
            }
            catch (Exception e) {
                Log.e("*INSERT*", e.toString());
            }
        }
		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {


        Log.i("*QUERY*", "querying key="+selection);
        String targetNode = getSuccessor(SHA1.genHash(selection)), data=null;
        switch (selection) {

            case "*":
                for(String nodeId: ports) {
                    Log.i("*QUERY*", "Collecting emulator-"+nodeId+" data...");
                    try {
                        data += sendReceive(nodeId, QUERY + SEPARATOR + "*", true) + SEPARATOR;
                    }
                    catch (Exception e) {
                        Log.i("*QUERY*", "Couldn't connect to emulator-"+nodeId);
                    }
                }
                break;

            case "@":
                data = "";
                for(String replica: DBOpener.REPLICAS) {
                    data += getPairsAsString(replica, null, null) + SEPARATOR;
                }
                targetNode = meHash;

            default:
                for(String nodeId: readMap(targetNode)) {
                    String temp;
                    try {
                        Log.i("*QUERY*", "Asking emulator-"+nodeId);
                        temp = sendReceive(nodeId, QUERY + SEPARATOR + selection + SEPARATOR + targetNode, true);
                        if (temp.length() == 0) continue;
                        data = (data != null) ? data+temp : temp;
                        break;
                    }
                    catch (Exception e) {
                        Log.i("*QUERY*", "Couldn't connect to emulator-"+nodeId+", trying another node...");
                    }
                }
        }
        if (data == null) {
            data = "";
            Log.e("*QUERY*", "FAILED querying key="+selection);
        }
        MatrixCursor mc = new MatrixCursor(new String[] {DBOpener.COLUMN_NAME_KEY, DBOpener.COLUMN_NAME_MESSAGE});
        for(String pair: data.split(SEPARATOR)) {
            String[] keyValue = pair.split(SPLITTER);
            if (keyValue.length != 2) continue;
            Log.i("GOT", "key="+keyValue[0]+" value="+keyValue[1]);
            mc.addRow(keyValue);
        }
        Log.i("TOTAL", " "+mc.getCount());

        return mc;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

    @Override
    public boolean onCreate() {
        DBOpener dbOpener = new DBOpener(getContext());
        mDB = dbOpener.getWritableDatabase();
        // clean up storage
        mDB.delete(DBOpener.MAIN_TABLE, null, null);
        for(String replica: DBOpener.REPLICAS) {
            mDB.delete(replica, null, null);
        }

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        me = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        meHash = SHA1.genHash(me);

        for(String port: ports) {
            String port_hash = SHA1.genHash(port);
            lookUp.add(port_hash);
            hash2port.put(port_hash, port);
        }
        indexing = new ArrayList<>(Arrays.asList(lookUp.toArray(new String[lookUp.size()])));
        String temp="";
        for(String nodeId: indexing)
            temp += hash2port.get(nodeId) + " ";
        Log.i(TAG, temp);

        try {
            new Server().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR,
                    new ServerSocket(serverPort)
            );
        }
        catch (IOException ioException) {
            Log.e(TAG, "error while creating ServerSocket");
        }

        new RecoverDB().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        new RecoverReplicas().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

        return !(mDB == null);
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }


    //***********************************************************************************
    // DYNAMO PROVIDER HELPERS
    //***********************************************************************************


    protected void insertStringPairs(String data, String table){
        ContentValues cv = new ContentValues();
        String[] pairs = data.split(SEPARATOR);
        for(String pair: pairs) {
            String[] keyValue = pair.split(SPLITTER);
            if (keyValue.length != 2) {
                Log.e("*insertStringPairs*", "inconsistent keyValue pair");
                break;
            }
            cv.put(DBOpener.COLUMN_NAME_KEY, keyValue[0]);
            cv.put(DBOpener.COLUMN_NAME_MESSAGE, keyValue[1]);
            if (mDB.replace(table, null, cv) == -1) {
                Log.e("*insertStringPairs*", "insert failed");
                break;
            }
        }
    }

    private String getPairsAsString(String table, String where, String[] whereArgs) {
        String out = "";
        Cursor all = mDB.query(table, null, where, whereArgs, null, null, null);
        if (all.getCount() > 0) {
            all.moveToFirst();
            while(!all.isLast()) {
                out += all.getString(0) + SPLITTER + all.getString(1) + SEPARATOR;
                all.moveToNext();
            }
            out += all.getString(0) + SPLITTER + all.getString(1);
        }
        all.close();
        return out;
    }

    private String getSuccessor(String hash) {
        String successor = lookUp.ceiling(hash);
        return (successor == null) ? lookUp.first() : successor;
    }

    private String sendReceive(String toWhom, String message, boolean receive) throws Exception {

        Socket server = new Socket();

        server.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(toWhom) * 2), 300);
        PrintStream streamWriter = new PrintStream(server.getOutputStream(), true);
        BufferedReader streamReader = new BufferedReader( new InputStreamReader(server.getInputStream()));
        streamWriter.println(message);
        if (streamWriter.checkError()) {
            Log.e("*SENDRECEIVE*", "couldn't send message");
            throw new Exception();
        }
        if (receive) {
            String reply = streamReader.readLine();
            if (reply == null) throw new Exception();
            return reply.trim();
        }
        return "";
    }

    private String findTable(String nodeHash) {
        int nodeIndex = indexing.indexOf(nodeHash);
        int meIndex = indexing.indexOf(meHash);

        // if target node is me then read from Main table of DB
        if (nodeIndex == meIndex) {
            return DBOpener.MAIN_TABLE;
        }

        // else find correct name of a replica table
        if (nodeIndex < meIndex) {
            return DBOpener.REPLICAS[meIndex - nodeIndex - 1];
        }
        return DBOpener.REPLICAS[(numberOfNodes-1) - nodeIndex + meIndex];
    }

    private String indexToNodeId(int index) {
        return hash2port.get(indexing.get(index));
    }

    private String[] readMap(String nodeHash) {
        String[] out = new String[NUMBER_OF_REPLICAS + 1];
        int nodeIndex = indexing.indexOf(nodeHash);
        for(int i = 0; i <= NUMBER_OF_REPLICAS; i++) {
            out[NUMBER_OF_REPLICAS - i] = indexToNodeId((nodeIndex + i) % numberOfNodes);
        }
        return out;
    }

    private String[] writeMap(String nodeHash) {
        String[] out = readMap(nodeHash);
        for(int i=0; i < out.length / 2; i++) {
            String temp = out[i];
            out[i] = out[out.length - 1 - i];
            out[out.length - 1 - i] = temp;
        }
        return out;
    }

    private String[] recoveryMap() {
        String[] out = new String[NUMBER_OF_REPLICAS];
        int nodeIndex = indexing.indexOf(meHash);
        for(int i = 1; i <= NUMBER_OF_REPLICAS; i++) {
            out[NUMBER_OF_REPLICAS - i] = indexToNodeId((nodeIndex + i) % numberOfNodes);
        }
        return out;
    }

    private String[] replicaRecoveryMap(String nodeHash) {
        String[] out = new String[NUMBER_OF_REPLICAS];
        int meIndex = indexing.indexOf(meHash);
        int nodeIndex = indexing.indexOf(nodeHash);
        int next = 0;
        for(int i=0; nodeIndex + i <= nodeIndex + NUMBER_OF_REPLICAS; i++) {
            int index = (nodeIndex + i) % numberOfNodes;
            if (index == meIndex) continue;
            out[NUMBER_OF_REPLICAS - next - 1] = indexToNodeId(index);
            next++;
        }
        return out;
    }

    // array of nodes I have replica for
    private String[] getReplicators() {
        String[] out = new String[NUMBER_OF_REPLICAS];
        int meIndex = indexing.indexOf(meHash);
        for(int i=1; i <= NUMBER_OF_REPLICAS; i++) {
            int index = meIndex - i;
            if (index < 0) {
                index = numberOfNodes + index;
            }
            out[i-1] = indexing.get(index);
        }
        return out;
    }



    //***********************************************************************************
    // DYNAMO PROVIDER CLASSES
    //***********************************************************************************



    private class RecoverDB extends AsyncTask<Void, Void, Void> {

        @Override
        protected Void doInBackground(Void... params) {

            String recoveryMessage;

            for(String nodeId: recoveryMap()) {
                try {
                    recoveryMessage = sendReceive(nodeId, RECOVERDB + SEPARATOR + meHash, true);
                    if (recoveryMessage.length() == 0) {
                        Log.i("*RECOVERDB*", "No key-value pairs to restore");
                        return null;
                    }
                    Log.i("*RECOVERDB*", "Restoring "+recoveryMessage.split(SEPARATOR).length+
                            " key-value pairs from emulator-"+nodeId);
                    insertStringPairs(recoveryMessage, DBOpener.MAIN_TABLE);
                    return null;
                }
                catch (Exception e) {
                    Log.i("*RECOVERDB*", "attempt failed, keep trying...");
                }
            }

            Log.e("*RECOVERDB*", "couldn't restore DB from replicas");

            return null;
        }
    }


    private class RecoverReplicas extends AsyncTask<Void, Void, Void> {

        @Override
        protected Void doInBackground(Void... params) {

            String[] replicators = getReplicators();
            for(int i=0; i < replicators.length; i++) {
                String recoveryMessage = null;
                for(String nodeId: replicaRecoveryMap(replicators[i])) {
                    try {
                        recoveryMessage = sendReceive(nodeId, RECOVERREPLICA + SEPARATOR + replicators[i], true);
                        if (recoveryMessage.length() == 0) {
                            Log.i("*RECOVERREPLICAS*",
                                    "No key-value pairs to replicate from emulator-"+nodeId+
                                            " for emulator-"+hash2port.get(replicators[i]));
                            break;
                        }
                        Log.i("*RECOVERREPLICAS*", "Replicating "+recoveryMessage.split(SEPARATOR).length+
                                " key-value pairs for emulator-"+
                                hash2port.get(replicators[i]));
                        insertStringPairs(recoveryMessage, DBOpener.REPLICAS[i]);
                        break;
                    }
                    catch (Exception e) {
                        Log.i("*RECOVERREPLICAS*", "attempt "+nodeId+" failed, keep trying...");
                    }
                }
                if (recoveryMessage == null)
                    Log.e("*RECOVERREPLICAS*", "couldn't restore replica for emulator-"+hash2port.get(replicators[i]));
            }

            return null;
        }
    }


    private class Server extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... params) {
            ServerSocket server = params[0];
            Socket client = null;
            BufferedReader streamReader = null;
            PrintStream streamWriter = null;
            String reply = "";
            while (true) {
                try {
                    // Set up connection
                    client = server.accept();
                    // Get the socket streams
                    streamReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    streamWriter = new PrintStream(client.getOutputStream(), true);
                    String[] message = streamReader.readLine().trim().split(SEPARATOR);
                    if (message.length < 2)
                        throw new Exception("too few arguments in the message");
                    switch (message[0]) {

                        case RECOVERDB:
                            reply = getPairsAsString(findTable(message[1]), null, null);
                            streamWriter.println(reply);
                            if (streamWriter.checkError())
                                throw new Exception("couldn't send RECOVERDB data");
                            break;

                        case RECOVERREPLICA:
                            reply = getPairsAsString(findTable(message[1]), null, null);
                            streamWriter.println(reply);
                            if (streamWriter.checkError())
                                throw new Exception("couldn't send RECOVERREPLICA data");
                            break;

                        case DELETE:
                            switch (message[1]) {

                                case "@":
                                    mDB.delete(findTable(message[2]), null, null);
                                    break;

                                default:
                                    mDB.delete(findTable(message[2]), "key=?", new String[] {message[1]});
                                    break;
                            }
                            break;

                        case DELETEALL:
                            mDB.delete(DBOpener.MAIN_TABLE, null, null);
                            for(String replica: DBOpener.REPLICAS) {
                                mDB.delete(replica, null, null);
                            }
                            break;

                        case INSERT:
                            insertStringPairs(message[1], findTable(message[2]));
                            break;

                        case QUERY:
                            switch (message[1]) {

                                case "*":
                                    for(String replica: DBOpener.REPLICAS) {
                                        reply += getPairsAsString(replica, null, null) + SEPARATOR;
                                    }
                                    reply += getPairsAsString(DBOpener.MAIN_TABLE, null, null);
                                    streamWriter.println(reply);
                                    break;

                                case "@":
                                    streamWriter.println(getPairsAsString(findTable(message[2]), null, null));
                                    break;

                                default:
                                    streamWriter.println(getPairsAsString(findTable(message[2]), "key=?", new String[]{message[1]}));
                                    break;

                            }
                            break;

                        default:
                            throw new Exception("unknown header");
                    }
                    client.close();
                }
                catch(Exception e) {
                    Log.e(TAG+" *SERVER*", e.toString());
                }
                finally {
                    try {
                        if (client != null) client.close();
                    }
                    catch(IOException ioException) {
                        Log.e(TAG+" *SERVER*", ioException.toString());
                    }
                }
            }
        }

        @Override
        protected void onProgressUpdate(String... values) {}

    }

}
