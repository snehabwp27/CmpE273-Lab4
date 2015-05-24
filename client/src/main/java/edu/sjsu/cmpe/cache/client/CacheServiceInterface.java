package edu.sjsu.cmpe.cache.client;

/**
 * Cache Service Interface
 *
 */
public interface CacheServiceInterface {

    public void get(long key);

    public void put(long key, String value);

    boolean delete(long key);
}