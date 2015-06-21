package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by archana on 3/31/15.
 */
public class GlobalConstants {

    public static final String TABLE_NAME ="DHT_Table";
    public static final String TEMP_TABLE ="Global_Table";
    public static final String MASTER = "11108";

    public static final String JOIN_TYPE = "join";
    public static final String RING_INFO = "maps";
    public static final String I_MESSAGE = "message";
    public static final String G_QUERY = "globalQuery";
    public static final String S_QUERY = "selectionQuery";
    public static final String DELETE = "deleteTable";
    public static final String DELETE_SEL = "selectionDelete";
    public static final String DELETED = "deletedValue";

    public static final String LOCAL_INDICATOR = "\"@\"";
    public static final String GLOBAL_INDICATOR = "\"*\"";

    public static final int SERVER_PORT = 10000;
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";



    public static String portStr;
    public static String myPort ;
    public static String myID;
    public static  String MASTER_KEY =null;
    public static List<String> ringList = null;
    public static boolean inRing = false;
    public static boolean isFirst= false;
    public static boolean isLast= false;
    public static boolean alldelete = false;
    public static boolean deleted = false;
    public static HashMap<String,String> succesorMap = new HashMap<String,String>();
    public static HashMap<String,String> predecessorMap = new HashMap<String,String>();
    public static HashMap<String,String> idToPortMap = new HashMap<String,String>();
    public static HashMap<String,String> globalValues = null;
    public static String returnedValue = null;
    public static Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    public static Context context = null;
    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    public static boolean checkNode(String key){
        String prev = null;
        prev = predecessorMap.get(myID);
        String next = null;
        next = succesorMap.get(myID);
       /* Log.e("checkNode P", prev);
        Log.e("checkNode N", next);*/
        String hashKey =null;
        try {
           hashKey= genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(prev==null && next==null){
            Log.e("checkNode", "if both null");
            return true;
        }else  if( (hashKey.compareTo(GlobalConstants.myID)<=0 && (hashKey.compareTo(prev) > 0 || GlobalConstants.myID.compareTo(prev) < 0 )) || (GlobalConstants.myID.compareTo(prev) < 0 && GlobalConstants.myID.compareTo(hashKey) < 0 && hashKey.compareTo(prev) > 0) ){
            Log.e("checkNode", "right place");
            return true;
        }else{
            Log.e("checkNode", "fwd");
            return false;
        }
    }
    public static void sendMessage(MessagePacket messagePacket) {
        Log.e("SEND FUNC", "SEND TO"+messagePacket.getSendTo());
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket);
    }
    public static HashMap<String,String> appendMaps(HashMap<String,String> first, HashMap<String,String> second){
        first.putAll(second);
        return first;
    }

    public static MatrixCursor getCursorfromMap(HashMap<String,String> map){
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{GlobalConstants.KEY_FIELD, GlobalConstants.VALUE_FIELD});
        Iterator it = map.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            Log.e("key",entry.getKey().toString());

            matrixCursor.addRow(new String[]{entry.getKey().toString(), entry.getValue().toString()});

        }

        Log.e("MAtrix",matrixCursor.toString());
        return matrixCursor;
    }

    public static HashMap<String,String> getDatafromCursor(Cursor obj) /*throws Exception*/{
        HashMap<String, String> result = new HashMap<String, String>();
        if(obj.getCount() !=0) {

            int keyIndex = obj.getColumnIndex(GlobalConstants.KEY_FIELD);
            int valueIndex = obj.getColumnIndex(GlobalConstants.VALUE_FIELD);
       /* if (keyIndex == -1 || valueIndex == -1) {
            Log.e("error", "Wrong columns");
            obj.close();
            throw new Exception();
        }*/
            obj.moveToFirst();
        /*if (!(obj.isFirst() && obj.isLast())) {
            Log.e("error", "Wrong number of rows");
            obj.close();
            throw new Exception();
        }*/
            String returnKey = null;
            String returnValue = null;
            while (!obj.isLast()) {
                returnKey = obj.getString(keyIndex);
                returnValue = obj.getString(valueIndex);
                result.put(returnKey, returnValue);
                obj.moveToNext();
            }
            //add the last entry
            returnKey = obj.getString(keyIndex);
            returnValue = obj.getString(valueIndex);
            result.put(returnKey, returnValue);
            return result;
        }
        return result;
    }
}
