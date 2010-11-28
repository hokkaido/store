(ns store.api
  (:require [clomert :as v]
            [ring.util.codec :as ring])
  (:use store.s3
        store.core
        [clojure.string :only [escape]]
        [plumbing.freezer :only [freeze thaw to-bytes from-bytes]]
        [plumbing.core :only [find-first map-from-keys retry -?> try-silent rpartial]])
  (:import [java.util.concurrent ConcurrentHashMap TimeoutException]
           [redis.clients.jedis JedisPool Jedis]))

(defprotocol IBucket
  (bucket-get [this k] "fetch value for key")
  (bucket-put [this k v] "write value for key")
  (bucket-keys [this] "seq of existing keys")
  (bucket-delete [this k] "remove key-value pair")
  (bucket-exists? [this k] "does key-value pair exists"))

;; Redis

(defmacro with-jedis-client
  [^JedisPool pool cname client-timeout retry-count & body]
  `(let [~(with-meta (symbol cname) {:tag Jedis}) (.getResource ~pool ~client-timeout)]
     (retry ~retry-count
            (fn [] (try ~@body
                        (finally (.returnResource ~pool ~cname)))))))

(defn- mk-key [bucket key] (format "%s:%s" bucket key))

(defn- default-bucket-exists? [b k]
  (find-first
   (partial = k)
   (bucket-keys b)))

(defn redis-bucket
  "returns callback fn for a Redis backed bucketbucket"
  [^String bucket 
   {:keys [host,port,timeout,pool-timeout,retry-count,num-clients]
    :or {host "127.0.0.1"
         port 6379
         timeout 50
         pool-timeout 50
         retry-count 10
         num-clients 20}
    :as spec}]
  (let [jedis-pool (doto (JedisPool. ^String host ^int port timeout)
                     (.setResourcesNumber ^int num-clients)
                     (.init))]
    (reify IBucket
           (bucket-get [this k]
                       (with-jedis-client jedis-pool c pool-timeout retry-count
                         (when-let [val (when (.exists c (mk-key bucket k))
                                          (.get c (mk-key bucket k)))]
                           (-> val .getBytes from-bytes))))
           (bucket-put [this k v]
                       (with-jedis-client jedis-pool c pool-timeout retry-count
                         (.set c (mk-key bucket k) (-> v to-bytes (String.)))
                         nil))
           (bucket-keys [this]
                        (with-jedis-client jedis-pool c pool-timeout retry-count
                          (let [prefix-ln (inc (.length bucket))]
                            (doall (map #(.substring % prefix-ln)
                                        (.keys c (format "%s:*" bucket)))))))
           (bucket-delete [this k]
                          (with-jedis-client jedis-pool c pool-timeout retry-count
                            (.del c (into-array [(mk-key bucket k)]))))
           (bucket-exists? [this k] (default-bucket-exists? this k)
                           #_(with-jedis-client jedis-pool c pool-timeout retry-count
                             (.exists c (mk-key bucket k)))))))

;; File System


(defn fs-bucket [dir-path]
  ; ensure directory exists
  (let [f (java.io.File. dir-path)]
    (.mkdirs f))
  (reify IBucket
         (bucket-get [this k]
                     (let [f (java.io.File. dir-path (ring/url-encode k))]
                       (when (.exists f) (thaw f))))
         (bucket-put [this k v]
                     (let [f (java.io.File. dir-path (ring/url-encode k))]
                       (freeze f v)))
         (bucket-exists? [this k]		
                         (let [f (java.io.File. dir-path (ring/url-encode k))]
                           (.exists f)))
         (bucket-delete [this k]
                        (let [f (java.io.File. dir-path (ring/url-encode  k))]
                          (.delete f)))
         (bucket-keys [this]
                      (for [f (.listFiles (java.io.File. dir-path))]
                        (ring/url-decode (.getName f))))))

;; ConcurrentHashMap

(defn hashmap-bucket
  []
  (let [h (ConcurrentHashMap.)]
    (reify IBucket
           (bucket-put [this k v]
                       (.put h k (to-bytes v)))
           (bucket-keys [this] (enumeration-seq (.keys h)))
           (bucket-get [this k]
                       (when-let [v (.get h k)]
                         (from-bytes v)))
           (bucket-delete [this k]
                          (.remove h k))
           (bucket-exists? [this k]
                           (.containsKey h k)))))

;; S3

(defn s3-bucket
  "Takes a S3 connection, a bucket name, and an optional map from logical
  bucket name to actual S3 bucket name."
  [s3 bucket-name]
  (reify IBucket
         (bucket-put [this k v]
		     (put-clj s3 bucket-name (str k) v))
         (bucket-keys [this]
		      (get-keys s3 bucket-name))
         (bucket-get [this k]
		     (get-clj s3 bucket-name (str k)))
         (bucket-delete [this k]
			(delete-object s3 bucket-name (str k)))	 
         (bucket-exists? [this k]
                         (some #(= k (.getKey %))
                               (-?> s3 (objects bucket-name (str k)) seq)))))

;; Generic Buckets

(defn read-write-bucket
  "make a bucket which uses the read-interface of one bucket and the write of the other"
  [read-bucket-impl write-bucket-impl]
  (reify IBucket
         (bucket-get [this k]
                     (bucket-get read-bucket-impl k))
         (bucket-put [this k v]
                     (bucket-put write-bucket-impl k v))	 
         (bucket-exists? [this k]
                         (bucket-exists? read-bucket-impl k))
         (bucket-delete [this k]
                        (bucket-delete write-bucket-impl k))
         (bucket-keys [this]
                      (bucket-keys read-bucket-impl))))

(defn copy-bucket [src dst]
  (doseq [k (bucket-keys src)]
    (bucket-put dst k (bucket-get src k))))

(defn mk-store [bucket-map]
  (fn [op bucket-name & args]
    (let [bucket (bucket-map bucket-name)
	  bucket-op (case op
                        :get bucket-get
                        :put bucket-put
                        :keys bucket-keys
                        :exists? bucket-exists?
                        :delete bucket-delete)]
      (when (nil? bucket)
	(throw (RuntimeException. (format "Bucket doesn't exist: %s" bucket-name))))
      (apply bucket-op bucket args))))

(def default-redis-config
     {:host "127.0.0.1"
      :port 6379
      :timeout 0
      :pool-timeout 500
      :retry-count 20
      :num-clients 20})

(defn mk-redis-store
  "A store where default bucket implementations
   is redis with a given server spec.

   See redis-bucket for docmentation about keys in spec"
  ([bucket-names spec]
     (mk-store (map-from-keys
		(fn [b] (redis-bucket b spec))
		bucket-names)))
  ([bucket-names] (mk-redis-store bucket-names default-redis-config)))

(defn mk-hashmap-store
  [bucket-names]
  (mk-store (map-from-keys (fn [b] (hashmap-bucket)) bucket-names)))


;; (defn mk-vstore
;;   [stores]
;;   (obj {:put (fn [bucket k v]
;; 	       (v/do-store
;; 		(stores (str bucket))
;; 		(:put k v)))
;; 	:get (fn [bucket k]
;; 	       (v/versioned-value (v/do-store
;; 				   (stores (str bucket))
;; 				   (:get k))))
;; 	:update (fn [bucket k v]
;; 		  (v/store-apply-update
;; 		   (stores (str bucket))
;; 		   (fn [client]
;; 		     (let [ver (v/store-get client k)
;; 			   val (v/versioned-value ver)]
;; 		       (v/store-conditional-put client
;; 						k
;; 						(v/versioned-set-value! ver (append
;; 									     [v
;; 									     val])))))))
;; 	:delete (fn [bucket k]
;; 		  (v/do-store
;; 		   (stores (str bucket))
;; 		   (:delete k)))}))
;;TODO: :exists? :keys