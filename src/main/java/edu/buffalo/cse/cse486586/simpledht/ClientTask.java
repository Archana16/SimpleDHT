package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by archana on 3/31/15.
 */
public class ClientTask extends AsyncTask<MessagePacket, Void, Void> {

    @Override
    protected Void doInBackground(MessagePacket... params) {
        MessagePacket messagePacket = params[0];
        Log.e("CLIENT", "NEW CLIENT");
        try {
            if(messagePacket.getType().equals(GlobalConstants.RING_INFO)){
                //THIS MULTICAST HAPPENS ONLY FROM MASTER
                Log.e(" CTEST","MULTICASTING");
                for(String node : GlobalConstants.ringList){
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
                }

            }else{
                Log.e(" CTEST","FWD MESSAGE TO"+messagePacket.getSendTo());
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(messagePacket.getSendTo()));
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(messagePacket);
                out.close();
                socket.close();
            }


        } catch (UnknownHostException e) {
            Log.e("CLIENT", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("client exception ", e.getMessage());
            e.printStackTrace();
        }
        return null;
    }
}
