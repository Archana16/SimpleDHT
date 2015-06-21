package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;

import javax.microedition.khronos.opengles.GL10;

/**
 * Created by archana on 3/31/15.
 */
public class ServerTask extends AsyncTask<ServerSocket, MessagePacket, Void> {

    @Override
    protected Void doInBackground(ServerSocket... params) {
        ServerSocket serverSocket = params[0];
        try {
            while(true){
                Socket socket = serverSocket.accept();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                MessagePacket Incoming = (MessagePacket) in.readObject();

                if(Incoming!= null){
                    String type = Incoming.getType();

                    if(type.equals(GlobalConstants.RING_INFO)){
                        //GOT CHORD INFO
                        GlobalConstants.inRing = true;
                        GlobalConstants.succesorMap = Incoming.getSuccesorMap();
                        GlobalConstants.predecessorMap = Incoming.getPredecessorMap();
                        GlobalConstants.idToPortMap = Incoming.getPortMap();
                        if(Incoming.getFirst().equals(GlobalConstants.myID))
                            GlobalConstants.isFirst =true;
                        if(Incoming.getLast().equals(GlobalConstants.myID))
                            GlobalConstants.isLast=true;
                        Log.e("Got succ map update",GlobalConstants.succesorMap.toString());
                        Log.e("Got pred map update",GlobalConstants.predecessorMap.toString());
                        Log.e("Got port map update",GlobalConstants.idToPortMap.toString());
                    }else if(type.equals(GlobalConstants.JOIN_TYPE)){
                        //GOT NODE JOIN REQUEST
                        Log.e("MASTER", "UPDATE THE RING");
                        processNodeJoinRequest(Incoming);

                    }else if(type.equals(GlobalConstants.I_MESSAGE)){

                        Log.e("Server Insert","FWDD INSERT MESSAGE");
                        ContentValues keyValueToInsert = new ContentValues();
                        keyValueToInsert.put(GlobalConstants.KEY_FIELD, Incoming.getMessageKey());
                        keyValueToInsert.put(GlobalConstants.VALUE_FIELD,Incoming.getMessageValue());
                        Log.e("Server Insert","CALL INSERT OF"+GlobalConstants.myPort);
                        GlobalConstants.context.getContentResolver().insert(GlobalConstants.uri,keyValueToInsert);

                    }else if(type.equals(GlobalConstants.G_QUERY)){

                        //RING COMPLETED
                        if(Incoming.getGlobalOriginator().equals(GlobalConstants.myID)){
                            Log.e("final map *",Incoming.getGlobalData().toString());
                            GlobalConstants.globalValues = Incoming.getGlobalData();
                        }else{
                            Cursor obj = GlobalConstants.context.getContentResolver().query(GlobalConstants.uri,null,GlobalConstants.LOCAL_INDICATOR,null, null);
                            try {
                                Log.e("G_Q", "querying local");
                                Log.e("TEST",GlobalConstants.getDatafromCursor(obj).toString());
                                Log.e("message map", Incoming.getGlobalData().toString());

                                Incoming.setGlobalData(GlobalConstants.appendMaps(Incoming.getGlobalData(),GlobalConstants.getDatafromCursor(obj)));
                            } catch (Exception e) {
                                Log.e("error","cursor error in server");
                                e.printStackTrace();
                            }

                            //Incoming.setSendTo(GlobalConstants.idToPortMap.get(Incoming.getGlobalOriginator()));
                            Incoming.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                            GlobalConstants.sendMessage(Incoming);
                        }

                    }else if(type.equals(GlobalConstants.S_QUERY)){

                        Log.e("SERVER", "TEST SELECTION QUERY");
                        if(Incoming.getGlobalOriginator().equals(GlobalConstants.myID)){
                            //RING COMPLETED
                            Log.e("SERVER", "RING COMPLETE");
                            GlobalConstants.returnedValue = Incoming.getMessageValue();
                        }else{
                            if(GlobalConstants.checkNode(Incoming.getSelectionKey())){
                                Log.e("SERVER", "SHOULD BE PRESENT");
                                Cursor obj =GlobalConstants.context.getContentResolver().query(GlobalConstants.uri,null,Incoming.getSelectionKey(),null,null);
                                try {
                                    Incoming.setMessageValue(GlobalConstants.getDatafromCursor(obj).get(Incoming.getSelectionKey()));
                                } catch (Exception e) {
                                    Log.e("error","cursor error in server");
                                    e.printStackTrace();
                                }
                                Incoming.setSendTo(GlobalConstants.idToPortMap.get(Incoming.getGlobalOriginator()));
                                //Incoming.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                                GlobalConstants.sendMessage(Incoming);

                            }else{
                                Log.e("SERVER", "VALUE NOT FOUND, FWD");
                                Incoming.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                                Log.e("SERVER", "SEND TO NEXT");
                                GlobalConstants.sendMessage(Incoming);
                            }

                        }

                    }else if(type.equals(GlobalConstants.DELETE)){
                        //RING COMPLETED
                        if(Incoming.getGlobalOriginator().equals(GlobalConstants.myID)){
                            Log.e("RING COMPLETED","DELETED ALL");
                            GlobalConstants.alldelete=true;
                        }else{
                            int rows = GlobalConstants.context.getContentResolver().delete(GlobalConstants.uri,GlobalConstants.LOCAL_INDICATOR,null);
                            Incoming.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                            GlobalConstants.sendMessage(Incoming);
                        }

                    }else if(type.equals(GlobalConstants.DELETE_SEL)){
                        if(GlobalConstants.checkNode(Incoming.getSelectionKey())){
                            Log.e("Found item","DELETED with SELECTED");
                            int rows = GlobalConstants.context.getContentResolver().delete(GlobalConstants.uri,Incoming.getSelectionKey(),null);
                            Incoming.setType(GlobalConstants.DELETED);
                            Incoming.setSendTo(GlobalConstants.idToPortMap.get(Incoming.getGlobalOriginator()));
                            GlobalConstants.sendMessage(Incoming);
                        }else{
                            Incoming.setSendTo(GlobalConstants.idToPortMap.get(GlobalConstants.succesorMap.get(GlobalConstants.myID)));
                            GlobalConstants.sendMessage(Incoming);
                        }

                    }else if(type.equals(GlobalConstants.DELETED)){
                        GlobalConstants.deleted = true;
                    }
                    else {
                        Log.e("ERR","UNHANDLED TYPE");
                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        return null;
    }

    public void  processNodeJoinRequest(MessagePacket messagePacket){
        GlobalConstants.ringList.add(messagePacket.getSenderKey());
        GlobalConstants.idToPortMap.put(messagePacket.getSenderKey(), messagePacket.getSender());
        UpdateMaps();
        Log.e("Master smap",GlobalConstants.succesorMap.toString());
        Log.e("Master pmap",GlobalConstants.predecessorMap.toString());
        Log.e("Master map", GlobalConstants.idToPortMap.toString());
        sendUpdates();
    }

    public void UpdateMaps(){
        Collections.sort(GlobalConstants.ringList);

        Log.e("Sorted list with master", GlobalConstants.ringList.toString());
        //first node
        GlobalConstants.succesorMap.put(GlobalConstants.ringList.get(0), GlobalConstants.ringList.get(1));
        GlobalConstants.predecessorMap.put(GlobalConstants.ringList.get(0),GlobalConstants.ringList.get(GlobalConstants.ringList.size()-1));
        //last node
        GlobalConstants.succesorMap.put(GlobalConstants.ringList.get(GlobalConstants.ringList.size()-1),GlobalConstants.ringList.get(0));
        GlobalConstants.predecessorMap.put(GlobalConstants.ringList.get(GlobalConstants.ringList.size() - 1), GlobalConstants.ringList.get(GlobalConstants.ringList.size()-2));
        for(int i=1; i <= GlobalConstants.ringList.size()-2;i++ ){
            GlobalConstants.succesorMap.put(GlobalConstants.ringList.get(i),GlobalConstants.ringList.get(i+1));
            GlobalConstants.predecessorMap.put(GlobalConstants.ringList.get(i), GlobalConstants.ringList.get(i - 1));
        }

    }

    public void sendUpdates(){
        MessagePacket messagePacket = new MessagePacket();
        messagePacket.populateMaps(GlobalConstants.succesorMap,GlobalConstants.predecessorMap,GlobalConstants.idToPortMap);
        messagePacket.setType(GlobalConstants.RING_INFO);
        Collections.sort(GlobalConstants.ringList);
        messagePacket.setFirst(GlobalConstants.ringList.get(0));
        messagePacket.setLast(GlobalConstants.ringList.get(GlobalConstants.ringList.size()-1));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messagePacket, null);
    }
}

