(ns store.api
  (:require [clomert :as v]
	    [clj-http.client :as client]
            [ring.util.codec :as ring]
	    [clojure.string :as str])
  (:use store.s3
        [plumbing.core]
	[clojure.contrib.json :only [read-json json-str]])
  (:import [java.util.concurrent ConcurrentHashMap TimeoutException]
           [redis.clients.jedis JedisPool Jedis]))

(defprotocol IBucket
  (bucket-get [this k] "fetch value for key")
  (bucket-put [this k v]
    "write value for key. return value can be anything")
  (bucket-keys [this] "seq of existing keys")
  (bucket-seq [this] "seq of [k v] elems")
  (bucket-delete [this k] "remove key-value pair")
  (bucket-exists? [this k] "does key-value pair exists"))

;; Redis

(defn- mk-key [bucket key] (format "%s:%s" bucket key))

;;TODO better defaulting.
(defn default-bucket-exists? [b k]
  (find-first
   (partial = k)
   (bucket-keys b)))

(defn default-bucket-seq [b]
  (for [k (bucket-keys b)]
    [k (bucket-get b k)]))

(defn default-bucket-keys [b]
  (map first (bucket-seq b)))

(defn- ensure-jedis-pool [jedis-pool  host  port timeout  num-clients]
  (when (nil? @jedis-pool)
    (swap! jedis-pool (fn [_] (doto (JedisPool. ^String host ^int port timeout)
			 (.setResourcesNumber ^int num-clients)
			 (.init))))))

(defn with-jedis-client
  [^JedisPool pool client-timeout retry-count f]  
  (when-let [c (.getResource pool client-timeout)]
    (let [res (-->> [c] f
		    (with-retries retry-count)
		    with-log)]      
      (.returnResource pool c)
      res)))

(defn redis-bucket
  "returns callback fn for a Redis backed bucketbucket"
  ([^String bucket 
    {:keys [host,port,timeout,pool-timeout,retry-count,num-clients]
     :as spec}]
     (let [jedis-pool (atom nil)]
       (reify IBucket
	      (bucket-get [this k]
		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
		  (with-jedis-client @jedis-pool  pool-timeout retry-count
		    (fn [^Jedis c]
		      (when-let [v (.get c (mk-key bucket k))]
		       (read-string v)))))

	      (bucket-put [this k v]
		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
		  (with-jedis-client @jedis-pool  pool-timeout retry-count
		    (fn [^Jedis c] (.set c (mk-key bucket k) (pr-str v)))))
	      
	      (bucket-keys [this]
		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
		  (with-jedis-client @jedis-pool pool-timeout retry-count
		    (fn [^Jedis c]
		      (let [prefix-ln (inc (.length bucket))]
			(doall (map #(.substring ^String % prefix-ln)
				    (.keys c (format "%s:*" bucket))))))))

	      (bucket-seq [this] (default-bucket-seq this))
	      	      
	      (bucket-delete [this k]
		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
		  (with-jedis-client @jedis-pool pool-timeout retry-count
		    (fn [^Jedis c] (.del c (into-array [(mk-key bucket k)])))))
	      
	      (bucket-exists? [this k]
		  (ensure-jedis-pool jedis-pool host port timeout num-clients)
		  (default-bucket-exists? this k)
		  #_(with-jedis-client @jedis-pool pool-timeout retry-count
		    (fn [^Jedis c] (.exists c (mk-key bucket k)))))))))

;; Riak

(defn riak-bucket [& {:keys [server,name,port,prefix,bucket-config]
		      :or {server "http://127.0.0.1"
			   prefix "riak"
			   bucket-config {:props {:n_val 2
						  :allow_mult true		       
						  :last_write_wins true}}
			   port 8098}}]
  ;; Bucket config
  (let [req-base [(str server ":" port) prefix (ring/url-encode name)]
	mk-path #(str/join "/" (concat req-base %&))
	mk-json (fn [o] {:body (.getBytes (json-str o) "UTF8")
			 :content-type "application/json" :accepts :json})]
    ;; IBucket Implementatin
    (reify IBucket
	   (bucket-get
	    [this k]
	    (-log> k str ring/url-encode mk-path client/get
		   :body #(read-json % false))
	   (bucket-put
	    [this k v]
	    (-> k str ring/url-encode mk-path (client/post (mk-json v))))  
	   (bucket-delete
	    [this k]
	    (-> k ring/url-encode mk-path client/delete))	  
	   (bucket-seq
	    [this]
	    (default-bucket-seq this))
	   (bucket-keys
	    [this]
	    (let [data (-> (mk-path)
			   (client/get {:query-params {"keys" "stream"}})						   :body
			   java.io.StringReader.
			   java.io.PushbackReader.)]
	      (loop [ks nil]
		(let [ch (.read data)]
		  (if (= ch -1)
		    ks
		    (do
		      (.unread data ch)
		      (recur (concat ks
				     (map ring/url-decode
					  (:keys (silent
						  read-json
						  data false)))))))))))
	   (bucket-exists?
	    [this k]
	    (default-bucket-exists? this k)))))

(comment
  (def b (riak-bucket :name "b"))
  (doseq [k (bucket-keys b)]
    (bucket-delete b k))
  (bucket-put b "k" "v")
  (bucket-keys b)
)

;; Voldemort

;; File System

(defn fs-bucket [^String dir-path]
  ; ensure directory exists
  (let [f (java.io.File. dir-path)]
    (.mkdirs f)
    (reify IBucket
	   (bucket-get [this k]
		       (let [f (java.io.File. f ^String (ring/url-encode k))]
			 (when (.exists f) (-> f slurp read-string))))
	   (bucket-put [this k v]
		       (let [f (java.io.File. f ^String(ring/url-encode k))]
			 (spit f (pr-str v))))
	   (bucket-exists? [this k]		
			   (let [f (java.io.File. f ^String (ring/url-encode k))]
			     (.exists f)))
	   (bucket-delete [this k]
			  (let [f (java.io.File. f ^String (ring/url-encode  k))]
			    (.delete f)))

	   (bucket-seq [this] (default-bucket-seq this))
	 
	   (bucket-keys [this]
			(for [^java.io.File c (.listFiles f)
			      :when (and (.isFile c) (not (.isHidden c)))]
			  (ring/url-decode (.getName c)))))))

;; ConcurrentHashMap

(defn hashmap-bucket
  ([]
     (let [h (ConcurrentHashMap.)]
       (hashmap-bucket h)))
  ([^java.util.concurrent.ConcurrentHashMap h]
     (reify IBucket
            (bucket-put [this k v]
                        (.put h k (pr-str v)))
            (bucket-keys [this] (enumeration-seq (.keys h)))
            (bucket-get [this k] (when-let [v (.get h k)] (read-string v)))
	    (bucket-seq [this] (for [^java.util.Map$Entry e (seq h)]
				 [(.getKey e) (read-string (.getValue e))]))
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
	 (bucket-seq [this] (default-bucket-seq this))	 
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
	 (bucket-seq [this] (bucket-seq read-bucket-impl))
         (bucket-exists? [this k]
                         (bucket-exists? read-bucket-impl k))
         (bucket-delete [this k]
                        (bucket-delete write-bucket-impl k))
         (bucket-keys [this]
                      (bucket-keys read-bucket-impl))))

(defn copy-bucket [src dst]
  (doseq [k (bucket-keys src)]
    (bucket-put dst k (bucket-get src k))))

(defn mk-store 
  "Make a store. The store must come with a bucket-map
   containing string keys to bucket implementation values. You
   can mix bucket implementations across bucket keys.

   Store supports the following operations
     (s :get \"bucket\" \"key\") return key in bucket
     (s :seq \"bucket\") return seq of [key val] elems in store.
     (s :exists? \"bucket\" \"key\")
     (s :delete \"bucket\" \"key\") delete [k v] in bucket
     (s :keys \"bucket\") returns seq of keys for bucket
     (s :bucket \"bucket\") returns bucket impl 
  The first 6  ops correspond to bucket-{get,seq,get,exists?,delete,keys} respectively
  on the specific bucket"    
  [bucket-map]
  (fn [op bucket-name & args]
    (let [bucket (bucket-map bucket-name)
	  bucket-op (case op
                        :get bucket-get
			:seq bucket-seq
			:bucket nil
                        :put bucket-put
                        :keys bucket-keys
                        :exists? bucket-exists?
                        :delete bucket-delete)]
      (when (nil? bucket)
	(throw (RuntimeException. (format "Bucket doesn't exist: %s" bucket-name))))
      (if (= :bucket op)
	bucket
	(apply bucket-op bucket args)))))

; This is for a single bucket, so only a single client per-bucket
(def default-redis-config
     {:host "127.0.0.1"
      :port 6379
      :timeout 0
      :pool-timeout 500
      :retry-count 20
      :num-clients 5})

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

(defn mk-riak-store
  "A store where default bucket implementations
   is Riak with a given Riak connection."
  ([bucket-names riak-config]
     (mk-store (map-from-keys
		(fn [k] (apply riak-bucket
			       (apply concat (assoc riak-config :name k))))
		bucket-names)))
  ([bucket-names] (mk-riak-store bucket-names nil)))

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