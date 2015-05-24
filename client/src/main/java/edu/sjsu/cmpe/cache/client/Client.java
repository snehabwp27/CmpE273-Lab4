package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");

        CRDTClient objCRDT = new CRDTClient();
        objCRDT.add("http://localhost:3000");
        objCRDT.add("http://localhost:3001");
        objCRDT.add("http://localhost:3002");


        //Test for Read Repair
        objCRDT.writeToAll(1, "a");
        Thread.sleep(30 * 1000);
        objCRDT.writeToAll(1, "b");
        Thread.sleep(30 * 1000);
        System.out.println("all servers: " + objCRDT.readFromAll(1));


        //Test for Write Rollback
        Thread.sleep(30 * 1000);
        objCRDT.writeToAll(2, "c");


        System.out.println("Exiting Cache Client.");
    }

}