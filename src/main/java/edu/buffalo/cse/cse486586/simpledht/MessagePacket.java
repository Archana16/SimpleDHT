package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by archana on 3/31/15.
 */
public class MessagePacket implements Serializable {

    public MessagePacket(){
        this.type = null;
        this.sender =null;
        this.sendTo = null;
        this.senderKey= null;
        this.succesorMap = null;
        this.predecessorMap = null;
        this.messageKey =null;
        this.messageValue=null;
        globalOriginator = null;
        selectionKey= null;
        first = null;
        last =null;
        this.maps = false;

    }
    private String type;
    private String sender;
    private String sendTo;
    private String senderKey;
    private String messageKey;
    private String messageValue;
    private String globalOriginator;
    private String selectionKey;
    private String first;
    private String last;
    private HashMap<String,String> succesorMap;
    private HashMap<String,String> predecessorMap;
    private HashMap<String,String> portMap;
    private HashMap<String,String> globalData;
    private boolean maps;

    public String getType(){
        return this.type;
    }
    public void setType(String type){
        this.type = type;
    }
    public String getSender(){
        return this.sender;
    }
    public void setSender(String sender){
        this.sender = sender;
    }
    public void setSendTo(String sendTo){
        this.sendTo = sendTo;
    }
    public String getSendTo(){
        return this.sendTo;
    }
    public void populateMaps(HashMap<String,String> smap,HashMap<String,String> pmap,HashMap<String,String> portMap){
        this.succesorMap = smap;
        this.predecessorMap = pmap;
        this.portMap = portMap;
    }
    public HashMap<String,String> getSuccesorMap(){
        return this.succesorMap;
    }
    public HashMap<String,String> getPredecessorMap(){
        return this.predecessorMap;
    }
    public HashMap<String,String> getPortMap(){
        return this.portMap;
    }



    public void setSenderKey(String key){
        this.senderKey = key;
    }
    public String getSenderKey(){
        return this.senderKey;
    }
    public void setMessageKey(String messageKey){
        this.messageKey= messageKey;
    }
    public String getMessageKey(){
        return this.messageKey;
    }
    public void setMessageValue(String messageValue){
        this.messageValue = messageValue;
    }
    public String getMessageValue(){
        return this.messageValue;
    }
    public void setGlobalOriginator(String originator){
        this.globalOriginator =originator;
    }
    public String getGlobalOriginator(){
        return this.globalOriginator;
    }
    public void setGlobalData(HashMap<String, String> globalData){
        this.globalData= globalData;
    }
    public HashMap<String,String> getGlobalData(){
        return this.globalData;
    }
    public void setSelectionKey(String selectionKey){
        this.selectionKey= selectionKey;
    }
    public String getSelectionKey(){
        return this.selectionKey;
    }
    public String getFirst(){
        return this.first;
    }
    public String getLast(){
        return this.last;
    }
    public void setFirst(String first){
        this.first = first;
    }
    public void setLast(String last){
        this.last = last;
    }

}
