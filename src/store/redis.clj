(ns store.redis
  (:use store.core
	[plumbing.core :only [-->> with-ex with-retries]])
  (:import [redis.clients.jedis JedisPool Jedis]))
           
(defn- mk-key [bucket key] (format "%s:%s" bucket key))

(defn- ensure-jedis-pool [jedis-pool  host  port timeout  num-clients]
  (when (nil? @jedis-pool)
    (swap! jedis-pool (fn [_] (doto (JedisPool. ^String host ^int port timeout)
			 (.setResourcesNumber ^int num-clients)
			 (.init))))))

(defn with-jedis-client
  [^JedisPool pool client-timeout retry-count f]  
  (when-let [c (.getResource pool client-timeout)]
    (let [res (with-ex (logger)
		(with-retries retry-count f) c)]      
      (.returnResource pool c)
      res)))

(def default-redis-config
     {:host "127.0.0.1"
      :port 6379
      :timeout 0
      :pool-timeout 500
      :retry-count 20
      :num-clients 5})

(defn redis-bucket
  "returns callback fn for a Redis backed bucketbucket"
  ([^String bucket 
    {:keys [host,port,timeout,pool-timeout,retry-count,num-clients]
     :as spec}]
     (let [jedis-pool (atom nil)]
       (reify store.core.IReadBucket
              (bucket-get [this k]
                          (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
                          (with-jedis-client @jedis-pool  pool-timeout retry-count
                            (fn [^Jedis c]
                              (when-let [v (.get c (mk-key bucket k))]
                                (read-string v)))))

              (bucket-keys [this]
                           (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
                           (with-jedis-client @jedis-pool pool-timeout retry-count
                             (fn [^Jedis c]
                               (let [prefix-ln (inc (.length bucket))]
                                 (doall (map #(.substring ^String % prefix-ln)
                                             (.keys c (format "%s:*" bucket))))))))

	      (bucket-seq [this] (default-bucket-seq this))

	      (bucket-exists? [this k]
                              (ensure-jedis-pool jedis-pool host port timeout num-clients)
                              (default-bucket-exists? this k)
                              #_(with-jedis-client @jedis-pool pool-timeout retry-count
                                  (fn [^Jedis c] (.exists c (mk-key bucket k)))))

	      store.core.IWriteBucket

	      (bucket-put [this k v]
                          (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
                          (with-jedis-client @jedis-pool  pool-timeout retry-count
                            (fn [^Jedis c] (.set c (mk-key bucket k) (pr-str v)))))
              


              (bucket-update [this k f]
                             (default-bucket-update this k f))

              
              (bucket-delete [this k]
                             (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
                             (with-jedis-client @jedis-pool pool-timeout retry-count
                               (fn [^Jedis c] (.del c (into-array [(mk-key bucket k)])))))
              
              (bucket-sync [this] nil)
              (bucket-close [this] (.destroy jedis-pool))))))
