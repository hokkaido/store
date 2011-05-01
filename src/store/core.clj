(ns store.core
  (:use plumbing.core
        [clojure.java.io :only [file]])
  (:require 
   [ring.util.codec :as ring]
   [clojure.string :as str]
   [clj-json.core :as json]
   [clj-time.coerce :as time.coerce])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.io File]))

(defprotocol IReadBucket
  (bucket-get [this k] "fetch value for key")
  (bucket-batch-get [this ks] "values for many keys, return map with ks")
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

(defn default-bucket-batch-get [b ks]
  (into {} (for [k ks] [k (bucket-get b k)])))

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

(defn fs-bucket
  ([path name] (fs-bucket (str (file path name))))
  ([dir-path]
  ;; ensure directory exists
      (let [f (if (string? dir-path)
		(file dir-path)
		dir-path)]
	(.mkdirs f)
	(reify
	 IReadBucket
	 (bucket-get [this k]
		     (let [f (File. f ^String (ring/url-encode k))]
		       (when (.exists f) (-> f slurp read-string))))
	 (bucket-batch-get [this ks] (default-bucket-batch-get this ks))
	 (bucket-seq [this] (default-bucket-seq this))     
	 (bucket-exists? [this k]		
			 (let [f (File. f ^String (ring/url-encode k))]
			   (.exists f)))
	 (bucket-keys [this]
		      (for [^File c (.listFiles f)
			    :when (and (.isFile c) (not (.isHidden c)))]
			(ring/url-decode (.getName c))))
	 (bucket-modified [this k]
			  (time.coerce/from-long
			   (.lastModified (File. dir-path 
						 ^String (ring/url-encode k)))))
      
	 IWriteBucket
	 (bucket-put [this k v]
		     (let [f (File. f ^String(ring/url-encode k))]
		       (spit f (pr-str v))))
	 (bucket-delete [this k]
			(let [f (File. f ^String (ring/url-encode  k))]
			  (.delete f)))
	 (bucket-update [this k f]
			(default-bucket-update this k f))
	 (bucket-sync [this] nil)
	 (bucket-close [this] nil)))))

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
       (bucket-batch-get [this ks] (default-bucket-batch-get this ks))
       
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

;;; Extend Read buckets to clojure maps

(def ^:private read-bucket-map-impls
     {:bucket-get (fn [this k] (this k))
      :bucket-seq (fn [this] (seq this))
      :bucket-batch-get (fn [this ks] (default-bucket-batch-get this ks))
      :bucket-keys (fn [this] (keys this))
      :bucket-exists? (fn [this k] (find this k))
      })

(doseq [c [clojure.lang.PersistentHashMap
	   clojure.lang.PersistentArrayMap
	   clojure.lang.PersistentStructMap]]
  (extend c IReadBucket read-bucket-map-impls))


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
    (bucket-batch-get [this k] (bucket-batch-get b k))
    (bucket-exists? [this k] (bucket-exists? b k))
    (bucket-keys [this] (bucket-keys b))
    (bucket-seq [this] (bucket-seq b))
    (bucket-modified [this k] (bucket-modified b k))))

(defn compose-buckets [read-b write-b]
  (reify           
    IWriteBucket
    (bucket-put [this k v] (bucket-put write-b k v))
    (bucket-delete [this k] (bucket-delete write-b k))
    (bucket-update [this k f] (bucket-update write-b k f))
    (bucket-sync [this] (bucket-sync write-b))
    (bucket-close [this] (bucket-close write-b))
    (bucket-merge [this k v] (bucket-merge write-b k v))
    (bucket-merger [this] (bucket-merger write-b))

    IReadBucket
    (bucket-get [this k] (bucket-get read-b k))
    (bucket-batch-get [this k] (bucket-batch-get read-b k))
    (bucket-exists? [this k] (bucket-exists? read-b k))
    (bucket-keys [this] (bucket-keys read-b))
    (bucket-seq [this] (bucket-seq read-b))
    (bucket-modified [this k] (bucket-modified read-b k))))

(defn bucket-merge-to!
  "merge takes (k to-value from-value)"
  [from to]
  {:pre [(or (map? from) (satisfies? IReadBucket from))
         (and (satisfies? IWriteBucket to))]}	 
  (doseq [[k v] (if (map? from) from
                    (bucket-seq from))]
    (bucket-merge to k v))
  to)

(defn with-multicast
  [buckets]
  (reify
    store.core.IWriteBucket
    (bucket-put [this k v]
                (doseq [b buckets]
                  (bucket-put b k v)))))

(defn add-write-listeners [b listener-buckets]
  (compose-buckets b     
     (with-multicast (cons b listener-buckets))))

(defn with-flush
  "takes a bucket that has with-merge and returns an in-memory bucket which will use bucket-merge to merge values using the flush-merge-fn and when bucket-sync is called on return bucket
  will flush memory bucket into underlying bucket using underyling bucket merge fn"
  ([b]
     (with-flush b (bucket-merger
                    (if (coll? b) (first b) b))))
  ([bucket flush-merge-fn]
     (let [buckets (if (coll? bucket) bucket [bucket])
           get-bucket #(with-merge (hashmap-bucket) flush-merge-fn)
           mem-bucket (java.util.concurrent.atomic.AtomicReference.
                       (get-bucket))
           do-flush! #(let [cur (.getAndSet mem-bucket (get-bucket))]
                        (doseq [b buckets]
                          (bucket-merge-to! cur b)))]
       (reify
         store.core.IWriteBucket
         (bucket-merge [this k v]
                       (bucket-merge (.get mem-bucket) k v))
         (bucket-update [this k f]
                        (bucket-update (.get mem-bucket) k f))
         (bucket-sync [this]
                      (do-flush!)
                      (map bucket-sync buckets))
         (bucket-close [this]
                       (do-flush!)
                       (map bucket-close buckets))))))

(defn caching-bucket [f merge-fn]
  (let [b (with-merge (hashmap-bucket) merge-fn)]
    (compose-buckets
     (reify
      store.core.IReadBucket
      (bucket-get [this k]
		  (or (bucket-get b k)	    
		      (do 
			(bucket-merge b k (f k))
			(bucket-get b k))))
      (bucket-batch-get [this ks] (default-bucket-batch-get this ks)))     
     b)))

(def read-ops
  {:get bucket-get
   :batch-get bucket-batch-get
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
   :exists? bucket-exists?
   :modified bucket-modified})

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