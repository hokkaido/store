(ns store.api
  (:use    plumbing.core)
  (:require 
            [ring.util.codec :as ring]
            [clojure.string :as str]
            [clj-json.core :as json])
  (:import [java.util.concurrent ConcurrentHashMap]))

(set! *warn-on-reflection* false)

(defprotocol IReadBucket
  (bucket-get [this k] "fetch value for key")
  (bucket-exists? [this k] "does key-value pair exists")
  (bucket-keys [this] "seq of existing keys")
  (bucket-seq [this] "seq of [k v] elems")
  (bucket-modified [this k] "joda datetime of key modification"))

(defprotocol IWriteBucket
  (bucket-put [this k v]
    "write value for key. return value can be anything")
  (bucket-delete [this k] "remove key-value pair")
  (bucket-update [this k f])
  (bucket-sync [this])
  (bucket-close [this])
  ;; optional merging functins
  (bucket-merge [this k v] "merge v into current value")
  (bucket-merger [this] "return the fn used to merge. should be 3 args of [k cur-val new-val]"))

;;; Default Bucket Operations

(defn default-bucket-exists? [b k]
  (find-first
   (partial = k)
   (bucket-keys b)))

(defn default-bucket-update [b k f]
  (->>  k
        (bucket-get b)
        f
        (bucket-put b k)))

(defn default-bucket-seq [b]
  (for [k (bucket-keys b)]
    [k (bucket-get b k)]))

(defn default-bucket-keys [b]
  (map first (bucket-seq b)))

(defn default-bucket-merge [b merge-fn k v]
  (bucket-update b k (fn [v-to] (merge-fn v-to v))))

;;; Simple Buckets

(defn fs-bucket [^String dir-path]
  ;; ensure directory exists
  (let [f (java.io.File. dir-path)]
    (.mkdirs f)
    (reify IReadBucket
           (bucket-get [this k]
                       (let [f (java.io.File. f ^String (ring/url-encode k))]
                         (when (.exists f) (-> f slurp read-string))))

           (bucket-seq [this] (default-bucket-seq this))
	   
           (bucket-exists? [this k]		
                           (let [f (java.io.File. f ^String (ring/url-encode k))]
                             (.exists f)))

	   (bucket-keys [this]
                        (for [^java.io.File c (.listFiles f)
                              :when (and (.isFile c) (not (.isHidden c)))]
                          (ring/url-decode (.getName c))))

	   IWriteBucket

	   (bucket-put [this k v]
                       (let [f (java.io.File. f ^String(ring/url-encode k))]
                         (spit f (pr-str v))))
           (bucket-delete [this k]
                          (let [f (java.io.File. f ^String (ring/url-encode  k))]
                            (.delete f)))

           
           (bucket-update [this k f]
                          (default-bucket-update this k f))
           (bucket-sync [this] nil)
           (bucket-close [this] nil))))

(defn hashmap-bucket
  ([]
     (let [h (ConcurrentHashMap.)]
       (hashmap-bucket h)))
  ([^java.util.concurrent.ConcurrentHashMap h]
     (reify IReadBucket

            (bucket-keys [this]
			 (enumeration-seq (.keys h)))
            (bucket-get [this k]
			(.get h k))
            (bucket-seq [this]
			(for [^java.util.Map$Entry e
				     (.entrySet h)]
			  [(.getKey e) (.getValue e)]))

	    (bucket-exists? [this k]
                            (.containsKey h k))

	    IWriteBucket
	    (bucket-put [this k v]
                        (.put h k v))
            (bucket-delete [this k]
                           (.remove h k))
            (bucket-update [this k f]
		(loop []
		  (let [v (.get h k) new-v (f v)			
			replaced? (cond
				    (nil? v) (nil? (.putIfAbsent h k new-v))
				    (nil? new-v) (or (nil? v) (.remove h k v))
				    :else (.replace h k v new-v))]
		    (when (not replaced?)
		      (recur)))))
            (bucket-sync [this] nil)
            (bucket-close [this] nil))))


;;; Generic Buckets

(defn copy-bucket [src dst]
  (doseq [k (bucket-keys src)]
    (bucket-put dst k (bucket-get src k))))

(defn bucket-inc [b k]
  (bucket-update
   b k
   (fnil inc 0)))

(defn with-merge [b merge-fn]
  (reify           
     IWriteBucket
     (bucket-put [this k v] (bucket-put b k v))
     (bucket-delete [this k] (bucket-delete b k))
     (bucket-update [this k f] (bucket-update b k f))
     (bucket-sync [this] (bucket-sync b))
     (bucket-close [this] (bucket-close b))
     (bucket-merge [this k v]
       (default-bucket-merge b (partial merge-fn k) k v))
     (bucket-merger [this] merge-fn)

     IReadBucket
     (bucket-get [this k] (bucket-get b k))
     (bucket-exists? [this k] (bucket-exists? b k))
     (bucket-keys [this] (bucket-keys b))
     (bucket-seq [this] (bucket-seq b))
     (bucket-modified [this k] (bucket-modified b k))))

(defn bucket-merge-to!
  "merge takes (k to-value from-value)"
  [from to]
  {:pre [(or (map? from) (satisfies? IReadBucket from))
	 (and (satisfies? IWriteBucket to))]}	 
  (doseq [[k v] (if (map? from) from
		    (bucket-seq from))]
    (bucket-merge to k v))
  to)

(defn with-flush
  "takes a bucket that has with-merge and returns an in-memory bucket which will use bucket-merge to merge values using the flush-merge-fn and when bucket-sync is called on return bucket
  will flush memory bucket into underlying bucket using underyling bucket merge fn"
  ([bucket flush-merge-fn]
     (let [get-bucket #(with-merge b flush-merge-fn)
	   mem-bucket (java.util.concurrent.atomic.AtomicReference. (get-bucket))
	   do-flush! #(let [cur (.getAndSet mem-bucket (hashmap-bucket))]
			(bucket-merge-to! cur bucket))]
       (reify
	store.api.IWriteBucket
	(bucket-merge [this k v] (bucket-merge (.get mem-bucket) k v))		      
	(bucket-sync [this]
	  (do-flush!)
	  (bucket-sync bucket))
	(bucket-close [this]
	  (do-flush!)
	  (bucket-close bucket))		 

	store.api.IReadBucket
	(bucket-get [this k] (bucket-get bucket k))
	(bucket-seq [this] (bucket-seq bucket))
	(bucket-keys [this] (bucket-keys bucket)))))
  
  ([b] (with-flush b (bucket-merger b))))

(def read-ops
     {:get bucket-get
      :seq bucket-seq
      :bucket (fn [bucket & args] bucket)
      :keys bucket-keys
      :get-ensure
      (fn [bucket key default-fn]
	(if-let [v (bucket-get bucket key)]
	  v
	  (let [res (default-fn)]
	    (bucket-put bucket key res)
	    res)))
      :exists? bucket-exists?})

(def write-ops
     {:put bucket-put
      :delete bucket-delete
      :merge bucket-merge
      :update bucket-update
      :sync bucket-sync
      :close bucket-close})

(defn bucket-op [ops get-bucket]
  (fn [op bucket-name & args]
    (let [bucket (get-bucket bucket-name op)
	  bucket-op (ops op)]
      (when (nil? bucket)
	(throw (RuntimeException.
		(format "Bucket doesn't exist: %s" bucket-name))))
      (apply bucket-op bucket args))))

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
     (s :get-ensure \"bucket\" \"key\" get-fn)
     (s :merge \"bucket\" \"key\" \"partial-val\")
  The first 6  ops correspond to bucket-{get,seq,get,exists?,delete,keys} respectively
  on the specific bucket"    
  ([bucket-map]
     (bucket-op
      (merge read-ops write-ops)
      (fn [bucket op]
	(bucket-map bucket))))
  ([reads writes]
     (bucket-op
      (merge read-ops write-ops)
      (fn [bucket op]
	(if (find read-ops op)
	  (reads bucket)
	  (writes bucket))))))

(defn hash-buckets [keyspace]
  (map-from-keys
   (fn [n] (hashmap-bucket))
   keyspace))