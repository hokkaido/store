(ns store.api
  (:require [clomert :as v])
  (:use store.s3
        store.core
        [work.core :only [retry]])
  (:import [java.util.concurrent ConcurrentHashMap TimeoutException]
           [redis.clients.jedis JedisPool Jedis]))

(defprotocol IBucket
  get [this k]
  put [this k v]
  keys [this]
  delete [this k]
  exists? [this k])

(defmacro with-jedis-client
  [^JedisPool pool cname client-timeout retry-count & body]
  `(let [~(with-meta (symbol cname) {:tag Jedis}) (.getResource ~pool ~client-timeout)]
     (retry ~retry-count
            (fn [] (try ~@body
                        (finally (.returnResource ~pool ~cname)))))))

(defn mk-redis-bucket
  [[{host :host
     port :port
     timeout :timeout}
    keyspace
    & {:keys [pool-timeout retry-count num-clients] :or {:pool-timeout 50
                                                         :retry-count 10
                                                         :num-clients 20}}]]
  (reify IBucket
         (get [this k]
              (with-jedis-client jedis-pool c pool-timeout retry-count
                (if-let [val (.get c (mk-key keyspace k))]
                  (read-string val)
                  nil)))
         (put [this k v]
              (with-jedis-client jedis-pool c pool-timeout retry-count
                (.set c (mk-key keyspace k) (pr-str v))))
         (keys [this]
               (with-jedis-client jedis-pool c pool-timeout retry-count
                 (let [prefix-ln (inc (.length n))]
                   (doall (map #(.substring % prefix-ln)
                               (.keys c (format "%s:*" n)))))))
         (delete [this k]
                 (with-jedis-client jedis-pool c pool-timeout retry-count
                   (.del c (into-array [(mk-key keyspace k)]))))
         (exists? [this k]
                  (with-jedis-client jedis-pool c pool-timeout retry-count
                    (.exists c (mk-key keyspace k))))))

(defn mk-hashmap-bucket
  []
  (let [h (ConcurrentHashMap.)]
    (reify
     (put [this k v]
          (.put h k v))
     (keys [this] (enumeration-seq (.keys h)))
     (get [k]
          (.get h k))
     (delete [k]
             (.remove h k))
     (exists? [n k]
              (.containsKey h k)))))

(defn mk-s3-bucket
  "Takes a S3 connection, a bucket name, and an optional map from logical
  bucket name to actual S3 bucket name."
  [s3 b & [m]]
  (let [m (or m identity)]
    (reify IBucket
           (put [this k v]
                (try-default nil put-clj s3 (m b) (str k) v))
           (keys [this]
                 (try-default nil
                              get-keys s3 (m b)))
           (get [this k]
                (try-default nil
                             get-clj s3 (m b) (str k)))
           (delete [this k]
                   (try-default nil
                                delete-object s3 (m b) (str k)))

           (exists? [this k]
                    (or (some #(= k (.getKey %))
                              (try-default nil
                                           (comp seq objects)
                                           s3 (m b) (str k)))
                        false)))))

(defn mk-store-set
  "Mapping of keyspace -> impl."
  [keyspace-impls])

;;; Old stuff below, swapping out as soon as all code is moved over to use new store buckets.

(defn obj [s]
  (fn [op & args]
    (let [f (s op)]
      (apply f args))))

(defn mk-store [s3 & [m]]
  (let [m (or m identity)]
    (obj {:put (fn [b v k]
                 (try-default nil put-clj s3 (m b) (str k) v))
          :keys (fn [b]
                  (try-default nil
                               get-keys s3 (m b)))
          :get (fn [b k]
                 (try-default nil
                              get-clj s3 (m b) (str k)))
          :update (fn [b v k]
                    (try-default nil
                                 append-clj s3 (m b) (str k) v))
          :delete (fn [b k]
                    (try-default nil
                                 delete-object s3 (m b) (str k)))

          :exists? (fn [b k]
                     (or (some #(= k (.getKey %))
                               (try-default nil
                                            (comp seq objects)
                                            s3 (m b) (str k)))
                         false))})))

(defn mk-store-cache [config]
  (let [factory (v/make-socket-store-client-factory
                 (v/make-client-config config))
        m (ConcurrentHashMap.)]
    (fn [client]
      (if-let [c (get m client)]
        c
        (let [c (v/make-store-client factory client)]
          (.put m client c)
          c)))))

(defn mk-rstore
  "Redis store."
  [{host :host
    port :port
    timeout :timeout}
   keyspaces
   & {:keys [pool-timeout retry-count num-clients] :or {:pool-timeout 50
                                                        :retry-count 10
                                                        :num-clients 20}}]
  (let [mk-key #(format "%s:%s" %1 %2)
        ^JedisPool jedis-pool (doto (JedisPool. host port timeout)
                                ;; Was getting NPE below
                                ;; (.setResourcesNumber num-clients)
                                (.init))
        keyspaces (apply hash-set keyspaces)
        ensure-valid-name (fn [f]
                            (fn [n & args]
                              (if (contains? keyspaces n)
                                (apply f (cons n args))
                                (throw (Exception. (format "Invalid store name, %s" n))))))]
    (obj {:put (-> (fn [n v k]
                     (with-jedis-client jedis-pool c pool-timeout retry-count
                       (.set c (mk-key n k) (pr-str v))))
                   ensure-valid-name)
          :keys (-> (fn [n]
                      (with-jedis-client jedis-pool c pool-timeout retry-count
                        (let [prefix-ln (inc (.length n))]
                          (doall (map #(.substring % prefix-ln)
                                      (.keys c (format "%s:*" n)))))))
                    ensure-valid-name)
          :get (-> (fn [n k]
                     (with-jedis-client jedis-pool c pool-timeout retry-count
                       (if-let [val (.get c (mk-key n k))]
                         (read-string val)
                         nil)))
                   ensure-valid-name)
          :delete (-> (fn [n k]
                        (with-jedis-client jedis-pool c pool-timeout retry-count
                          (.del c (into-array [(mk-key n k)]))))
                      ensure-valid-name)
          :exists? (-> (fn [n k]
                         (with-jedis-client jedis-pool c pool-timeout retry-count
                           (.exists c (mk-key n k))))
                       ensure-valid-name)})))

(defn mk-chmstore
  [store-names]
  (let [stores (zipmap store-names
                       (repeatedly (ConcurrentHashMap.)))]
    (obj {:put (fn [n v k]
                 (.put (stores n) k v))
          :keys (fn [n]
                  (enumeration-seq (.keys (stores n))))
          :get (fn [n k]
                 (.get (stores n) k))
          :delete (fn [n k]
                    (.remove (stores n) k))
          :exists? (fn [n k]
                     (.containsKey (stores n) k))})))
       
(defn mk-vstore
  [stores]
  (obj {:put (fn [bucket k v]
	       (v/do-store
		(stores (str bucket))
		(:put k v)))
	:get (fn [bucket k]
	       (v/versioned-value (v/do-store
				   (stores (str bucket))
				   (:get k))))
	:update (fn [bucket k v]
		  (v/store-apply-update
		   (stores (str bucket))
		   (fn [client]
		     (let [ver (v/store-get client k)
			   val (v/versioned-value ver)]
		       (v/store-conditional-put client
						k
						(v/versioned-set-value! ver (append
									     [v
									     val])))))))
	:delete (fn [bucket k]
		  (v/do-store
		   (stores (str bucket))
		   (:delete k)))}))
;;TODO: :exists? :keys

;;(mk-vstore (mk-store-cache {:bootstrap-urls "tcp://localhost:6666"}))