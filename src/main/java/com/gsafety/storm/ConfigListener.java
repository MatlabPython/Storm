package com.gsafety.storm;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

/**
 * Author: huangll
 * Written on 17/9/20.
 */
public class ConfigListener implements PathChildrenCacheListener {
  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

  }
}
