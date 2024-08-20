package org.example.assignment3;
/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2607016
 *
 */

import java.io.*;
//import java.io.OutputStreamWriter;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;

import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
    //Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
    //these clasess are not exported by the module. Instead, one needs to impor
    //javax.sql.rowset.* as above.



public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome   = null;
    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;
    private CachedRowSet crs;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){
        serviceSocket = aSocket;
        this.start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop

		String tmp = "";
        try {
            InputStream socketStream = serviceSocket.getInputStream();
            InputStreamReader socketReader = new InputStreamReader(socketStream);
            StringBuffer stringBuffer = new StringBuffer();
            char x;
            while (true){
                System.out.println("Service thread: reading characters ");
                x = (char) socketReader.read();
                System.out.println("Service thread: " + x);
                if (x == '#')
                    break;
                stringBuffer.append(x);
            }
            tmp = stringBuffer.toString();
            this.requestStr = tmp.split(";");
         }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;

		
		String sql = "SELECT record.title, record.label, record.genre, record.rrp, COUNT(*)\n" +
                "FROM record\n" +
                "INNER JOIN artist ON artist.artistid = record.artistid\n" +
                "INNER JOIN recordcopy on recordcopy.recordid = record.recordid\n" +
                "INNER JOIN recordshop ON recordshop.recordshopid = recordcopy.recordshopid\n" +
                "WHERE artist.lastname =  ? AND recordshop.city = ?\n" +
                "GROUP BY record.title, record.label, record.genre, record.rrp;";
		try {
			Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            PreparedStatement psmt = connection.prepareStatement(sql);
            psmt.setString(1, this.requestStr[0]);
            psmt.setString(2, this.requestStr[1]);
            outcome = psmt.executeQuery();
            RowSetFactory aFactory = RowSetProvider.newFactory();
            crs = aFactory.createCachedRowSet();
            crs.populate(outcome);
            if (outcome == null){
                flagRequestAttended = false;
            }
            connection.close();
            psmt.close();
		} catch (Exception e) {
            System.out.println(e); System.exit(1); }
        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
            OutputStream outcomeStream = serviceSocket.getOutputStream();
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(outcomeStream);
            outcomeStreamWriter.writeObject(crs);
            outcomeStreamWriter.flush();
            while (crs.next()){
                System.out.print(crs.getString("Title") + " | ");
                System.out.print(crs.getString("Label") + " | ");
                System.out.print(crs.getString("Genre") + " | ");
                System.out.print(crs.getString("RRP") + " | ");
                System.out.println(crs.getString("Count") + " | ");
            }
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);
            serviceSocket.close();
        }catch (IOException | SQLException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
    }


    //The service thread run() method
    public void run() {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
        System.exit(0);
    }

}
