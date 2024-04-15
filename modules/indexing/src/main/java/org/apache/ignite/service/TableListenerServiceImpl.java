package org.apache.ignite.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * 监听ignite内存
 * @author liushengxue
 * @date 2024.04.07
 */
public class TableListenerServiceImpl implements Service {
    @IgniteInstanceResource
    private Ignite ignite;

    /** Service context. */
    @ServiceContextResource
    private ServiceContext ctx;
    /** Underlying cache map. */
    private IgniteCache<String, LocalDateTime> cache;

    private UUID remoteListenId;

    private ExecutorService executor = null;

//    private static final String CACHE_SCHEMA_NAME="TIDAVA_INFOS";
    private static final String CACHE_SCHEMA_NAME="PUBLIC";


    private static final String CACHE_TABLE_NAME="TIDAVA_TABLE_INFOS";

    /**
     * 记录缓存表信息的表名
     */
    private static final String CACHE_RECORD_TABLE_NAME = CACHE_SCHEMA_NAME+"."+CACHE_TABLE_NAME;
    private static final String CACHE_SCHEMA_TABLE_NAME="cache_"+CACHE_SCHEMA_NAME+"_"+CACHE_TABLE_NAME;
    private static final String DUMMY_CACHE_NAME = TableListenerServiceImpl.class.getSimpleName()+CACHE_RECORD_TABLE_NAME;

    /**
     * Service initialization.
     */
    @Override
    public void init() {
        try {
            System.out.println("init event TableListenerServiceImpl: ");
            cache = ignite.getOrCreateCache(CACHE_SCHEMA_TABLE_NAME);
            //初始化建表时间记录表
            initCacheTable();
            getExecutor();
            // 监听器逻辑...
            listenEvent();
            System.out.println("init event TableListenerServiceImpl end");
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    private void getExecutor() {
        // 线程池核心线程数（固定大小）
        int corePoolSize = 2;
        // 线程池最大线程数（与核心线程数相同，保持固定大小）
        int maximumPoolSize = 2;
        // 空闲线程存活时间（由于固定大小，此处设为0，即不超时）
        long keepAliveTime = 0L;
        // 时间单位（由于存活时间为0，此处可忽略）
        TimeUnit unit = TimeUnit.MILLISECONDS;

        // 使用无界任务队列（与 newFixedThreadPool 默认相同）
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();

        // 使用默认的 ThreadFactory（与 newFixedThreadPool 默认相同）
        ThreadFactory threadFactory = Executors.defaultThreadFactory();

        // 使用默认的 RejectedExecutionHandler（与 newFixedThreadPool 默认相同）
        RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();

        // 创建固定大小为 2 的线程池
        executor = new ThreadPoolExecutor(
               corePoolSize,
               maximumPoolSize,
               keepAliveTime,
               unit,
               workQueue,
               threadFactory,
               handler
       );
    }

    private void initCacheTable(){
            // Create reference City table based on REPLICATED template.
            cache.query(new SqlFieldsQuery(
                    "CREATE TABLE  IF NOT EXISTS "+CACHE_RECORD_TABLE_NAME+" (ID VARCHAR,TABLE_NAME VARCHAR,CACHE_NAME VARCHAR,CREATE_TIME VARCHAR, LAST_USE_TIME VARCHAR,PRIMARY KEY (ID, TABLE_NAME)) WITH \"backups=1,affinity_key=TABLE_NAME\"")).getAll();
    }

    private void listenEvent() {
        IgniteEvents events =ignite.events();
        IgnitePredicate<CacheEvent> filter = evt -> {
            return true;
        };

        // Subscribe to the cache events on all nodes where the cache is hosted.
        remoteListenId = events.remoteListen(new IgniteBiPredicate<UUID, CacheEvent>() {

            @Override
            public boolean apply(UUID uuid, CacheEvent e) {
                String cacheName=e.cacheName();
                if(null!=cacheName && cacheName.startsWith("SQL_PUBLIC_") && !cacheName.equals(CACHE_SCHEMA_TABLE_NAME) && !cacheName.equals("SQL_PUBLIC_"+CACHE_TABLE_NAME)){
                    executor.execute(() -> {
                        doEventDeal(e, cacheName);
                    });

                }else {
                    return true;
                }

                return true; //continue listening
            }
        }, null, EventType.EVT_CACHE_STARTED,EventType.EVT_CACHE_STOPPED);
        System.out.println("remoteListenId:"+remoteListenId);
    }

    private void doEventDeal(CacheEvent e, String cacheName) {
        String tableName= cacheName.substring("SQL_PUBLIC_".length());
        // process the event
        System.out.println("event cacheName: " + e.cacheName()+" ename:"+ e.name()+" etype:"+ e.type()+" tableName:"+tableName);
        if(null==ignite){
            return;
        }
        if(e.type()==EventType.EVT_CACHE_STARTED) {
            //新增表,不重复插入
            insertCacheTable(cacheName,tableName);
        }else if(e.type()==EventType.EVT_CACHE_STOPPED) {
            //清除表
            deleteTable(tableName);
        }
//        else {
//            //更新使用时间
//            updateLastUseTime(tableName);
//        }
    }

    /**
     * 创建插入表记录，重复不插入
     * @param cacheName
     * @param tableName
     */
    private void insertCacheTable(String cacheName,String tableName){
            List<List<?>> hasExistsList =cache.query(new SqlFieldsQuery(
                    "SELECT TABLE_NAME FROM "+CACHE_RECORD_TABLE_NAME+" where TABLE_NAME=\'"+tableName+"\'")).getAll();
            if(hasExistsList.isEmpty()){
                SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO "+CACHE_RECORD_TABLE_NAME+" (ID,TABLE_NAME,CACHE_NAME,CREATE_TIME,LAST_USE_TIME)" +
                        " VALUES (?,?,?,?,?)");

                cache.query(qry.setArgs(UUID.randomUUID(),tableName,cacheName,LocalDateTime.now(),LocalDateTime.now()));
            }
    }

    /**
     * 修改表最后使用时间
     * @param tableName
     */
    private void updateLastUseTime(String tableName) {
//                String sql =
//                        "update "+CACHE_RECORD_TABLE_NAME+" set LAST_USE_TIME = CURRENT_TIMESTAMP " +
//                                "where TABLE_NAME = ?";
//                SqlFieldsQuery updateQuery =   new SqlFieldsQuery(sql).setArgs(tableName);
//                cache.query(updateQuery);

    }

    /**
     * 消费删除表记录
     * @param tableName
     */
    private void deleteTable(String tableName) {
            cache.query(new SqlFieldsQuery("delete from "+CACHE_RECORD_TABLE_NAME+" where TABLE_NAME =\'"+tableName+"\'")).getAll();
    }


    /**
     * Cancel this service.
     */
    @Override
    public void cancel() {
        IgniteEvents events =ignite.events();
        events.stopRemoteListen(remoteListenId);
        // 关闭线程池，不再接受新任务
        executor.shutdown();
    }

    @Override
    public void execute() {
        // Since our service is simply represented by a counter value stored in a cache,
        // there is nothing we need to do in order to start it up.
        System.out.println("Executing distributed service");
    }
}
