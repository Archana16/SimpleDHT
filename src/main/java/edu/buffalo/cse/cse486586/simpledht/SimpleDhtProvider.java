package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import javax.microedition.khronos.opengles.GL10;

public class SimpleDhtProvider extends ContentProvider {

    public  SQLiteDatabase DHT_sqlDB;

    public static Cursor cObj;

    private class ClientTask extends AsyncTask<MessagePacket, Void, Void> {
        @Override
        protected Void doInBackground(MessagePacket... params) {
            MessagePacket messagePacket = params[0];
            Log.e("CLIENT", "NEW CLIENT");
            try {

                if(messagePacket.getType().equals(GlobalConstants.RING_INFO)){

                    for(String node : GlobalConstants.ringList) {
                            try {
                                String port = GlobalConstants.idToPortMap.get(node);
                                if(port.equals(GlobalConstants.MASTER))
                                    continue;
                                else{
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(port));
                                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                    out.writeObject(messagePacket);
                                    out.close();
                                    socket.close();
                                }
                            }catch(IOException e){
                                Log.e("client exception ",e.getMessage());
                            }

                    }
                }else {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(messagePacket.getSendTo()));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(messagePacket);
                        out.close();
                        socket.close();
                }
            }catch (UnknownHostException e) {
                Log.e("clientP", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("clientP", "client exception unicast "+e.getMessage());
            }
            catch(Exception e){
                Log.e("clientP","some exception");
            }

            return null;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals(GlobalConstants.GLOBAL_INDICATOR)){
            int rows= DHT_sqlDB.delete(GlobalConstants.TABLE_NAME,null,null);
            MessagePacket messagePacket= new MessagePacket();
            messagePacket.setType(GlobalConstants.DELETE);
            messagePacket.setGlobalOriginator(GlobalConstants.myID);
            messagePacket.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
            while(!GlobalConstants.alldelete);
            GlobalConstants.alldelete = false;
            return rows;
        }else if(selection.equals(GlobalConstants.LOCAL_INDICATOR)){
            int rows= DHT_sqlDB.delete(GlobalConstants.TABLE_NAME,null,null);
            return rows;
        }else{
            Log.e("D TEST", "INSIDE string DELETE" + selection);
            if (!GlobalConstants.checkNode(selection)) {
                Log.e("D TEST", "NOT INSIDE MY DB");
                MessagePacket messagePacket = new MessagePacket();
                messagePacket.setType(GlobalConstants.DELETE_SEL);
                messagePacket.setSelectionKey(selection);
                messagePacket.setGlobalOriginator(GlobalConstants.myID);
                messagePacket.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);

                while(!GlobalConstants.deleted);
                GlobalConstants.deleted=false;
                return 0;
            }else{
                Log.e("D TEST", "INSIDE MY DB");
                String where=GlobalConstants.KEY_FIELD+"='"+selection+"'";
                int rows= DHT_sqlDB.delete(GlobalConstants.TABLE_NAME,where,null);
                return rows;
            }
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.get(GlobalConstants.KEY_FIELD).toString();
        String value = values.get(GlobalConstants.VALUE_FIELD).toString();
        Log.e("Icheck","IN Insert of" +GlobalConstants.myPort);
        boolean insert = GlobalConstants.checkNode(key);
        if(insert){
            Log.e("Icheck","Can Insert in" +GlobalConstants.myPort);
            long row = DHT_sqlDB.insertWithOnConflict(
                    GlobalConstants.TABLE_NAME,
                    null,
                    values,5);
            if(row >0){
                Log.v("insert", values.toString());
                Uri newUri = ContentUris.withAppendedId(uri, row);
                getContext().getContentResolver().notifyChange(newUri, null);
                return newUri;
            }else{
                Log.v("insert", "failed");
                return uri;
            }
        }else{
            Log.e("Icheck","Cannot Insert in" +GlobalConstants.myPort);
            //FWD THE MESSAGE TO SUCCESSOR
            MessagePacket messagePacket = new MessagePacket();
            messagePacket.setMessageKey(key);
            messagePacket.setMessageValue(value);
            Log.e("I FWD TO", GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
            messagePacket.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
            messagePacket.setType(GlobalConstants.I_MESSAGE);
            //GlobalConstants.sendMessage(messagePacket);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
        }
        return null;

    }

    public void sendJoinMessage(){
        MessagePacket messagePacket = new MessagePacket();
        messagePacket.setType(GlobalConstants.JOIN_TYPE);
        messagePacket.setSenderKey(GlobalConstants.myID);
        messagePacket.setSender(GlobalConstants.myPort);
        messagePacket.setSendTo(GlobalConstants.MASTER);
        //GlobalConstants.sendMessage(messagePacket);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);

    }
    public void initializePorts(){


        Log.e("Initialize ports", "init");
        //IDENTIFY MY_PORT AND MY_ID
        GlobalConstants.context = getContext();
        TelephonyManager tel = (TelephonyManager) GlobalConstants.context.getSystemService(GlobalConstants.context.TELEPHONY_SERVICE);
        GlobalConstants.portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        GlobalConstants.myPort = String.valueOf((Integer.parseInt(GlobalConstants.portStr))*2);
        try {
            GlobalConstants.myID = GlobalConstants.genHash(String.valueOf(Integer.parseInt(GlobalConstants.portStr)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.e("port initialised to", GlobalConstants.myPort);
        if(!GlobalConstants.myPort.equals(GlobalConstants.MASTER)){
            //if(GlobalConstants.inRing)
            sendJoinMessage();
        }
        else {
            GlobalConstants.ringList= new ArrayList<String>();
            GlobalConstants.ringList.add(GlobalConstants.myID);
            GlobalConstants.MASTER_KEY = GlobalConstants.myID;
            GlobalConstants.idToPortMap.put(GlobalConstants.myID,GlobalConstants.myPort);
            Log.e("init ringlist", GlobalConstants.ringList.toString());
        }


    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Log.e("avd created", "in onCreate");
        initializePorts();
        //CREATE SERVER TASK

        ServerTask serverTask = new ServerTask();
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(GlobalConstants.SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        serverTask.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        //create DB
        DHTdatabase dbContext = new DHTdatabase(GlobalConstants.context);
        DHT_sqlDB = dbContext.getWritableDatabase();
        return (DHT_sqlDB==null)?false:true;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        Log.e("TEST","INSIDE QUERY");
        String query = null;
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        qBuilder.setTables(GlobalConstants.TABLE_NAME);
        //QUERY LOCAL
        query =  "SELECT  * FROM " + GlobalConstants.TABLE_NAME;
        cObj = DHT_sqlDB.rawQuery(query, null);
        //cObj.setNotificationUri(getContext().getContentResolver(), uri);
        Log.v("query", "queried local db");

        if(selection.equals(GlobalConstants.GLOBAL_INDICATOR)){

            if(GlobalConstants.succesorMap.get(GlobalConstants.myID)==null && GlobalConstants.predecessorMap.get(GlobalConstants.myID)==null){
                return cObj;
            }else {

                Log.e("Q TEST", "INSIDE * QUERY");
                //initiate ring query and populate globalvalue
                MessagePacket messagePacket = new MessagePacket();
                messagePacket.setGlobalOriginator(GlobalConstants.myID);
                messagePacket.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                messagePacket.setType(GlobalConstants.G_QUERY);
                messagePacket.setGlobalData(GlobalConstants.getDatafromCursor(cObj));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);


                GlobalConstants.globalValues = null;
                //wait until you get back all the values
                Log.e("Q TEST", "WAITING FOR ALL");
                while (GlobalConstants.globalValues == null) ;
                Log.e("Q TEST", "GOT DATA");
                MatrixCursor matrixCursor = GlobalConstants.getCursorfromMap(GlobalConstants.globalValues);
                GlobalConstants.globalValues = null;
                return matrixCursor;
            }
            /*cObj = new MergeCursor(new Cursor[]{matrixCursor, cObj});
            cObj.moveToFirst();
            Log.e("Q TEST", "RETURN APPENDED CURSOR");
            return cObj;*/
        }
        else if(selection.equals(GlobalConstants.LOCAL_INDICATOR)){
            Log.e("Q TEST","INSIDE @ QUERY");
            return cObj;
        }else {
            Log.e("Q TEST", "INSIDE string QUERY" + selection);
            if (!GlobalConstants.checkNode(selection)) {

                Log.e("Q TEST", "NOT INSIDE MY DB");
                MessagePacket messagePacket = new MessagePacket();
                messagePacket.setType(GlobalConstants.S_QUERY);
                messagePacket.setSelectionKey(selection);
                messagePacket.setGlobalOriginator(GlobalConstants.myID);
                messagePacket.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);

                Log.e("Q TEST", "WAIT");
                while (GlobalConstants.returnedValue == null) ;
                Log.e("Q TEST", "GOT THE CORRESPONDING VALUE" + GlobalConstants.returnedValue);

                HashMap<String, String> map = new HashMap<String, String>();
                map.put(selection, GlobalConstants.returnedValue);
                GlobalConstants.returnedValue = null;
                Log.e("map", map.toString());
                MatrixCursor matrixCursor = GlobalConstants.getCursorfromMap(map);
                Log.e("returned MAtrix",matrixCursor.toString());

                return matrixCursor;
                //not required
                /*cObj = new MergeCursor(new Cursor[]{matrixCursor, cObj});
                Log.e("Q TEST", "RETURN APPENDED CURSOR");
                cObj.moveToFirst();
                return cObj;*/

            } else {
                Log.e("Q TEST", "INSIDE MY DB");
                query = "SELECT  * FROM " + GlobalConstants.TABLE_NAME+ " WHERE key='"+selection+"'";
                cObj = DHT_sqlDB.rawQuery(query, null);
                cObj.setNotificationUri(getContext().getContentResolver(), uri);
                Log.v("query", "queried local db");
                return cObj;

            }
        }

    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }




}
