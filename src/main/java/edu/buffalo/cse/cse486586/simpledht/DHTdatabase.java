package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by archana on 3/29/15.
 */
public class DHTdatabase extends SQLiteOpenHelper {

    DHTdatabase(Context context){
        super(context,"PA3",null,3);
    }
    @Override
    public void onCreate(SQLiteDatabase db) {

        db.execSQL("CREATE TABLE "+GlobalConstants.TABLE_NAME+"("+GlobalConstants.KEY_FIELD+" TEXT UNIQUE, "+GlobalConstants.VALUE_FIELD+" TEXT NOT NULL);");


    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        db.execSQL("DROP TABLE IF EXISTS "+GlobalConstants.TABLE_NAME);

        onCreate(db);

    }

    // TODO : NOT ABLE TO CALL WHEN REQUIRED SO CREATED IN onCREATE
    public void createTempTable(SQLiteDatabase db){
        db.execSQL("CREATE TABLE "+GlobalConstants.TEMP_TABLE+"("+GlobalConstants.KEY_FIELD+" TEXT UNIQUE, "+GlobalConstants.VALUE_FIELD+" TEXT NOT NULL);");
    }

    public void deleteTempTable(SQLiteDatabase db){
        db.execSQL("DROP TABLE IF EXISTS "+GlobalConstants.TEMP_TABLE);

    }
}
