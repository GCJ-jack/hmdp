package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    @Override
    public Result queryById(Long id) {
//        // 解决缓存穿透
//        Shop shop = cacheClient
//                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        // 互斥锁解决缓存击穿
//        // Shop shop = cacheClient
//        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        // 逻辑过期解决缓存击穿
//        // Shop shop = cacheClient
//        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);
//
//        if (shop == null) {
//            return Result.fail("店铺不存在！");
//        }
//        // 7.返回
//        return Result.ok(shop);

        //根据id查询店铺的时，如果缓存未命中，将数据库结果写入缓存， 并设置超时时间

//        String key = CACHE_SHOP_KEY + id;
//        // 1.从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在
//        if(StrUtil.isNotBlank(shopJson)){
//            //3.存在，直接返回
//            Shop shop = JSONUtil.toBean((shopJson), Shop.class);
//            return Result.ok(shop);
//        }
//        //4.不存在，根据id查询数据库
//
//        //实现缓存重建
//        //获取互斥锁
//        //判断是否获得成功
//        //失败则休眠重试
//        Shop shop = getById(id);
//        //5.不存在，返回结果
//        if(shop == null){
//            return Result.fail("店铺不存在！");
//        }
//        //存在，写入redis
//        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);

        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);
        //返回
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
//        Long id = shop.getId();
//        if (id == null) {
//            return Result.fail("店铺id不能为空");
//        }
//        // 1.更新数据库
//        updateById(shop);
//        // 2.删除缓存
//        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
//        return Result.ok();
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        x = null;
        y = null;
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }

    public void saveShop2Redis(Long id, Long expireSeconds) {
        //1.查询店铺状态
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    public Shop queryWithMutex(Long id)  {
//        String key = CACHE_SHOP_KEY + id;
//        // 1、从redis中查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2、判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 存在,直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断命中的值是否是空值
//        if (shopJson != null) {
//            //返回一个错误信息
//            return null;
//        }
//        // 4.实现缓存重构
//        //4.1 获取互斥锁
//        String lockKey = "lock:shop:" + id;
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(lockKey);
//            // 4.2 判断否获取成功
//            if(!isLock){
//                //4.3 失败，则休眠重试
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//            //4.4 成功，根据id查询数据库
//            shop = getById(id);
//            Thread.sleep(200);
//            // 5.不存在，返回错误
//            if(shop == null){
//                //将空值写入redis
//                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
//                //返回错误信息
//                return null;
//            }
//            //6.写入redis
//            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_NULL_TTL,TimeUnit.MINUTES);
//
//        }catch (Exception e){
//            throw new RuntimeException(e);
//        }
//        finally {
//            //7.释放互斥锁
//            unlock(lockKey);
//        }
//        return shop;

        String key = CACHE_SHOP_KEY + id;
        //1.从redis中读取
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在

        if (StrUtil.isNotBlank(shopJson)) {
            //存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        if(shopJson != null){
            //返回一个错误信息
            return null;
        }
        // 4.实现缓存重构
        //4.1 获取互斥锁
        String lockKey = "lock:shop" + id;
        Shop shop = null;

        try{
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取锁成功
            if(!isLock){
                //4.3失败，则休眠重试
                Thread.sleep(50);
                //重试
                return queryWithMutex(id);
            }
            //成功 根据id查询数据库
            shop = getById(id);
            //不存在，返回错误
            if(shop == null){
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_NULL_TTL,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7.释放互斥锁
            unlock(lockKey);
        }

        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }


    public Shop queryWithLogicalExpire( Long id ) {
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isBlank(json)) {
            // 3.存在，直接返回
            return null;
        }

        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject)redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回店铺信息
            return shop;
        }
        // 5.2.已过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2.判断是否获取锁成功
        if (isLock){
            CACHE_REBUILD_EXECUTOR.submit( ()->{

                try{
                    //重建缓存
                    this.saveShop2Redis(id,20L);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    unlock(lockKey);
                }
            });
        }
        // 6.4.返回过期的商铺信息
        return shop;
    }

//    @Override
//    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
//        //判断是否需要坐标查询
//        if (x == null || y == null) {
//            //不需要坐标查询
//            Page<Shop> page = lambdaQuery()
//                    .eq(Shop::getTypeId, typeId)
//                    .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
//            return Result.ok(page.getRecords());
//        }
//        //计算分页参数
////        int from = (current - 1) * SystemConstants.MAX_PAGE_SIZE;
////        int end = current * SystemConstants.MAX_PAGE_SIZE;
//        int pageSize = SystemConstants.MAX_PAGE_SIZE;
//        int from = (current - 1) * pageSize;
//        int toFetch = from + pageSize;
//        //查询redis 距离排序 分页
//        String key= SHOP_GEO_KEY+typeId;
//        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
//                .search(key
//                        , GeoReference.fromCoordinate(x, y)
//                        , new Distance(5000)
//                        , RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(toFetch)
//                );
//        //解析出id
//        if (results==null){
//            return Result.ok(Collections.emptyList());
//        }
//        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();
//        if (content.size() <= from) {
//            return Result.ok(Collections.emptyList());
//        }
//        //截取
//        List<Long> ids=new ArrayList<>(content.size());
//        Map<String,Distance> distanceMap=new HashMap<>();
//        content.stream().skip(from).forEach(result->{
//            //店铺id
//            String shopId = result.getContent().getName();
//            ids.add(Long.valueOf(shopId));
//            //距离
//            Distance distance = result.getDistance();
//            distanceMap.put(shopId,distance);
//        });
//        //根据id查询shop
//        String join = StrUtil.join(",", ids);
//        List<Shop> shopList = lambdaQuery().in(Shop::getId, ids).last("order by field(id,"+join+")").list();
//        for (Shop shop : shopList) {
//            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
//        }
//        return Result.ok(shopList);
//    }
}
