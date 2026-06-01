package com.kingpixel.cobbleutils.util.cache;

import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes Redis invalidation messages to registered {@link CrossServerCache} instances by channel id.
 */
public final class CrossServerCacheRegistry {

  private static final Map<String, CrossServerCache<?, ?>> CACHES = new ConcurrentHashMap<>();

  private CrossServerCacheRegistry() {
  }

  static void register(@NotNull CrossServerCache<?, ?> cache) {
    CACHES.put(cache.getChannelId(), cache);
  }

  public static void dispatch(@NotNull JsonObject json) {
    if (!json.has(CrossServerCache.FIELD_CHANNEL)) {
      return;
    }
    String channelId = json.get(CrossServerCache.FIELD_CHANNEL).getAsString();
    CrossServerCache<?, ?> cache = CACHES.get(channelId);
    if (cache != null) {
      cache.handleRemoteInvalidation(json);
    }
  }
}
