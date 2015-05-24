package edu.sjsu.cmpe.cache.client;

/**
 * Created by sneha on 5/23/15.
 */

    import java.util.ArrayList;
    import java.util.Collections;
    import java.util.HashSet;
    import java.util.Set;
    import java.util.concurrent.ConcurrentHashMap;

    public class CRDTClient {

        public ConcurrentHashMap<String, String> writeState = new ConcurrentHashMap<String, String>();
        public ConcurrentHashMap<String, String> readState = new ConcurrentHashMap<String, String>();
        private ArrayList<DistributedCacheService> servers = new ArrayList<DistributedCacheService>();

        public void add(String serverURL) {
            servers.add(new DistributedCacheService(serverURL, this));
        }


        public void writeToAll(long key, String value) {

            int failures = 0;

            for (DistributedCacheService ser : servers) {
                ser.put(key, value);
            }

            do {

                if (writeState.size() >= servers.size()) {

                    for (DistributedCacheService server : servers) {
                        System.out.println("Writing to " + server.getCacheServerURL() + ": " + writeState.get(server.getCacheServerURL()));
                        if (writeState.get(server.getCacheServerURL()).equalsIgnoreCase("fail"))
                            failures++;
                    }

                    if (failures > 1) {
                        System.out.println("Rollback!!");
                        for (DistributedCacheService server : servers) {
                            server.delete(key);
                        }
                    }
                    writeState.clear();
                    break;
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (true);
        }

        public String readFromAll(long key) throws InterruptedException {

            for (DistributedCacheService server : servers) {
                server.get(key);
            }
            Set<DistributedCacheService> failedServers = new HashSet<DistributedCacheService>();
            Set<DistributedCacheService> consistentServers = new HashSet<DistributedCacheService>(servers);
            consistentServers.addAll(servers);


            while (true) {
                if (readState.size() < 3) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {


                    System.out.println(readState);
                    for (DistributedCacheService server : servers) {

                        if (readState.get(server.getCacheServerURL()).equalsIgnoreCase("fail")) {
                            System.out.println("Failure at : " + server.getCacheServerURL());
                            failedServers.add(server);
                        }
                    }

                    consistentServers.removeAll(failedServers);
                    System.out.println("consistent -->" + consistentServers);
                    Thread.sleep(500);
                    String valueToAdd = null;

                    if (failedServers.size() > 0) {


                        System.out.println("failed Servers -->" + failedServers);
                        ArrayList<String> allValues = new ArrayList<String>();
                        ArrayList<DistributedCacheService> allServers = new ArrayList<DistributedCacheService>();

                        for (DistributedCacheService consServ : consistentServers) {
                            String temp = consServ.getSynchronous(key);
                            allValues.add(temp);
                            allServers.add(allValues.indexOf(temp), consServ);
                        }


                        Set<String> unique = new HashSet<String>(allValues);
                        int max = Integer.MIN_VALUE;
                        DistributedCacheService maxServer = null;

                        for (String val : unique) {
                            int currMax = Collections.frequency(allValues, val);
                            if (currMax > max) {
                                max = currMax;
                                valueToAdd = val;

                            }
                        }

                        System.out.println("making the servers consistent.");

                        for (DistributedCacheService ser : failedServers) {
                            System.out.println("right value for server: " + ser.getCacheServerURL() + " as: " + valueToAdd);
                            ser.putSynchronous(key, valueToAdd);
                        }
                        failedServers.clear();

                        readState.clear();

                        return valueToAdd;
                    }
                    failedServers.clear();

                }
            }

        }

    }

