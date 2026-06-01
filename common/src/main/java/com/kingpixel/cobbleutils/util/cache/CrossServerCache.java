package com.kingpixel.cobbleutils.util.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.gson.JsonObject;
import com.kingpixel.cobbleutils.CobbleUtils;
import com.kingpixel.cobbleutils.util.redis.RedisManager;
import com.kingpixel.cobbleutils.util.redis.handlers.RedisCrossServerCacheHandler;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Local Caffeine cache with optional Redis-backed invalidation across server instances.
 */
public final class CrossServerCache<K, V> {

  public static final String ACTION_INVALIDATE = "invalidate";
  public static final String FIELD_ACTION = "action";
  public static final String FIELD_CHANNEL = "channel";
  public static final String FIELD_KEY = "key";
  public static final String FIELD_ORIGIN = "origin";

  public static final String INSTANCE_ID = java.util.UUID.randomUUID().toString();

  @Getter
  private final String channelId;
  private final Cache<K, V> cache;
  private final Function<K, String> keyEncoder;
  private final Function<String, K> keyDecoder;

  private CrossServerCache(
    String channelId,
    Cache<K, V> cache,
    Function<K, String> keyEncoder,
    Function<String, K> keyDecoder
  ) {
    this.channelId = Objects.requireNonNull(channelId, "channelId");
    this.cache = Objects.requireNonNull(cache, "cache");
    this.keyEncoder = Objects.requireNonNull(keyEncoder, "keyEncoder");
    this.keyDecoder = Objects.requireNonNull(keyDecoder, "keyDecoder");
    CrossServerCacheRegistry.register(this);
  }

  public static <K, V> Builder<K, V> builder(@NotNull String channelId) {
    return new Builder<>(channelId);
  }

  @Nullable
  public V getIfPresent(@NotNull K key) {
    return cache.getIfPresent(key);
  }

  public void put(@NotNull K key, @NotNull V value) {
    cache.put(key, value);
  }

  public void invalidateLocal(@NotNull K key) {
    cache.invalidate(key);
  }

  public void invalidateAndBroadcast(@NotNull K key) {
    invalidateLocal(key);
    publishInvalidation(key);
  }

  public ConcurrentMap<K, V> asMap() {
    return cache.asMap();
  }

  void handleRemoteInvalidation(@NotNull JsonObject json) {
    try {
      String origin = json.has(FIELD_ORIGIN) ? json.get(FIELD_ORIGIN).getAsString() : null;
      if (INSTANCE_ID.equals(origin)) {
        return;
      }
      if (!ACTION_INVALIDATE.equals(json.has(FIELD_ACTION) ? json.get(FIELD_ACTION).getAsString() : null)) {
        return;
      }
      if (!json.has(FIELD_KEY)) {
        return;
      }
      K key = keyDecoder.apply(json.get(FIELD_KEY).getAsString());
      if (key != null) {
        cache.invalidate(key);
        if (CobbleUtils.config != null && CobbleUtils.config.isDebug()) {
          CobbleUtils.LOGGER_RAW.info("Cross-server cache invalidated: {} / {}", channelId, key);
        }
      }
    } catch (Exception e) {
      CobbleUtils.LOGGER_RAW.error("Error handling cross-server cache invalidation for " + channelId, e);
    }
  }

  private void publishInvalidation(@NotNull K key) {
    try {
      if (CobbleUtils.config == null || !CobbleUtils.config.isRedisMessaging()) {
        return;
      }
      RedisManager mgr = CobbleUtils.redisManager;
      if (mgr == null || !mgr.getConnected().get()) {
        return;
      }
      JsonObject json = new JsonObject();
      json.addProperty(FIELD_ACTION, ACTION_INVALIDATE);
      json.addProperty(FIELD_CHANNEL, channelId);
      json.addProperty(FIELD_KEY, keyEncoder.apply(key));
      json.addProperty(FIELD_ORIGIN, INSTANCE_ID);
      mgr.publish(RedisCrossServerCacheHandler.IDENTIFIER, json);
    } catch (Exception e) {
      if (CobbleUtils.config != null && CobbleUtils.config.isDebug()) {
        CobbleUtils.LOGGER_RAW.error("Failed to publish cache invalidation for " + channelId, e);
      }
    }
  }

  public static final class Builder<K, V> {
    private final String channelId;
    private long expireAfterWriteMinutes = 5;
    private long maximumSize = 500;
    private Function<K, String> keyEncoder;
    private Function<String, K> keyDecoder;

    private Builder(String channelId) {
      this.channelId = channelId;
    }

    public Builder<K, V> expireAfterWrite(long duration, @NotNull TimeUnit unit) {
      this.expireAfterWriteMinutes = unit.toMinutes(duration);
      return this;
    }

    public Builder<K, V> maximumSize(long maximumSize) {
      this.maximumSize = maximumSize;
      return this;
    }

    public Builder<K, V> keyCodec(
      @NotNull Function<K, String> encoder,
      @NotNull Function<String, K> decoder
    ) {
      this.keyEncoder = encoder;
      this.keyDecoder = decoder;
      return this;
    }

    public CrossServerCache<K, V> build() {
      if (keyEncoder == null || keyDecoder == null) {
        throw new IllegalStateException("keyCodec(encoder, decoder) is required");
      }
      Cache<K, V> built = Caffeine.newBuilder()
        .expireAfterWrite(expireAfterWriteMinutes, TimeUnit.MINUTES)
        .maximumSize(maximumSize)
        .build();
      return new CrossServerCache<>(channelId, built, keyEncoder, keyDecoder);
    }
  }
}
