package com.kingpixel.cobbleutils.util.redis.handlers;

import com.google.gson.JsonObject;
import com.kingpixel.cobbleutils.util.cache.CrossServerCacheRegistry;

/**
 * Redis handler for {@link com.kingpixel.cobbleutils.util.cache.CrossServerCache} invalidation messages.
 */
public class RedisCrossServerCacheHandler implements RedisHandler {

  public static final String IDENTIFIER = "cross-server-cache";

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public void handle(JsonObject json) {
    CrossServerCacheRegistry.dispatch(json);
  }
}
