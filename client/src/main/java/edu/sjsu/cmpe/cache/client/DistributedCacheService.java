package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Distributed cache service
 */
public class DistributedCacheService implements CacheServiceInterface {

    private final String cacheServerUrl;

    ConcurrentHashMap<String, String> status = new ConcurrentHashMap<String, String>();

    CRDTClient crdt;

    public DistributedCacheService(String serverUrl) {
        this.cacheServerUrl = serverUrl;
    }

    public DistributedCacheService(String serverUrl, ConcurrentHashMap<String, String> status) {
        this.cacheServerUrl = serverUrl;
        this.status = status;
    }

    public DistributedCacheService(String serverUrl, CRDTClient crdt) {
        this.cacheServerUrl = serverUrl;
        this.crdt = crdt;
    }

    public String getCacheServerURL() {
        return this.cacheServerUrl;
    }

    @Override
    public void get(long key) {
        Future<HttpResponse<JsonNode>> future = Unirest.get(this.cacheServerUrl + "/cache/{key}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        System.out.println("GET request failed");
                        crdt.readState.put(cacheServerUrl, "fail");
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                        if (response.getCode() != 200) {
                            crdt.readState.put(cacheServerUrl, "fail");
                        } else {
                            String value = response.getBody().getObject().getString("value");
                            System.out.println("GET value from server: " + cacheServerUrl + ": " + value);
                            crdt.readState.put(cacheServerUrl, value);
                        }
                    }

                    public void cancelled() {
                        System.out.println("GET request cancelled");
                        crdt.readState.put(cacheServerUrl, "fail");
                    }

                });
    }


    @Override
    public void put(long key, String value) {
        System.out.println("put : " + key + ", " + value);
        Future<HttpResponse<JsonNode>> future = Unirest.put(this.cacheServerUrl + "/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .routeParam("value", value)
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        System.out.println("PUT request failed!!!");
                        crdt.writeState.put(cacheServerUrl, "fail");
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                        if (response == null || response.getCode() != 200) {
                            System.out.println("Failed to add to the cache.");
                            crdt.writeState.put(cacheServerUrl, "fail");
                        } else {
                            System.out.println("PUT request successfull");
                            crdt.writeState.put(cacheServerUrl, "pass");
                        }
                    }

                    public void cancelled() {
                        System.out.println("PUT request cancelled");
                        crdt.writeState.put(cacheServerUrl, "fail");
                    }

                });
    }


    @Override
    public boolean delete(long key) {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest.delete(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }

        if (response == null || response.getCode() != 204) {
            System.out.println("Cannot be deleted");
            return false;
        } else {
            System.out.println("Deleted!!!");
            return true;
        }
    }

    @Override
    public String toString() {
        return this.cacheServerUrl;
    }

    public void putSynchronous(long key, String value) {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest
                    .put(this.cacheServerUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }

        if (response.getCode() != 200) {
            System.out.println("Failed to add to cache");
        }
    }

    public String getSynchronous(long key) {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest.get(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }
        String value = null;
        if (response.getCode() != 204) {

            value = response.getBody().getObject().getString("value");
        }

        return value;
    }
}