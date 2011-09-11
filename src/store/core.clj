(ns store.core
  (:use plumbing.core
	[plumbing.error :only [assert-keys]]
        [clojure.java.io :only [file]])
  (:require 
   [ring.util.codec :as ring]
   [clojure.string :as str]
   [clj-json.core :as json]
   [clj-time.coerce :as time.coerce]
   [clojure.contrib.logging :as log])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.io File]))

(defprotocol IReadBucket
  (bucket-get [this k] "fetch value for key")
  (bucket-batch-get [this ks] "return seq of [k v] pairs")
  (bucket-exists? [this k] "does key-value pair exists")
  (bucket-keys [this] "seq of existing keys")
  (bucket-seq [this] "seq of [k v] elems")
  (bucket-count [this] "the number of kv pairs in this bucket"))

(defprotocol IWriteBucket
  (bucket-put [this k v]
              "write value for key. return value can be anything")
  (bucket-batch-put [this kvs] "put a seq of [k v] pairs")
  (bucket-delete [this k] "remove key-value pair")
  (bucket-update [this k f])
  (bucket-sync [this])
  (bucket-close [this]))

(defprotocol IMergeBucket
  (bucket-merge [this k v] "merge v into current value")
  (bucket-batch-merge [this kvs] "merge key valye pairs kvs into current values"))

(defprotocol IOptimizeBucket
  (bucket-optimize [this] "optimize for in order reads from disk on :keys and :seq requests"))
  
;;; Default Bucket Operations
(defn default-bucket-exists? [b k]
  (find-first
   (partial = k)
   (bucket-keys b)))

(defn default-bucket-batch-put [b kvs]
  (doseq [[k v] kvs] (bucket-put b k v)))

(defn default-bucket-batch-get [b ks]
  (for [k ks] [k (bucket-get b k)]))

;;TODO: put on the protocol, implementations can be much more efficient deleting with cursor.
(defn clear [b]
  (doseq [k (bucket-keys b)]
    (bucket-delete b k)))

(defn default-bucket-update [b k f]
  (->>  k
        (bucket-get b)
        f
        (bucket-put b k)))

(defn default-bucket-seq [b]
  (for [k (bucket-keys b)]
    [k (bucket-get b k)]))

(defn default-bucket-merge [b merge-fn k v]
  (assert merge-fn)
  (bucket-update b k (fn [v-to] (merge-fn k v-to v))))

(defn default-bucket-batch-merge [b merge-fn kvs]
  (assert merge-fn)
  (doseq [[k v] kvs]
    (bucket-update b k (fn [v-to] (merge-fn k v-to v)))))

(defn hashmap-bucket [^ConcurrentHashMap h & [merge-fn]]
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
	   (bucket-exists? [this k] (.containsKey h k))
	   (bucket-count [this] (.size h))

	   IMergeBucket
	   (bucket-merge [this k v]
			 (default-bucket-merge this merge-fn k v))
	   (bucket-batch-merge [this kvs]
			 (default-bucket-batch-merge this merge-fn kvs))

	   IWriteBucket
	   (bucket-put [this k v]
		       (.put h k v))
	   (bucket-batch-put [this kvs] (default-bucket-batch-put this kvs))
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
	   (bucket-close [this] nil)))

(defn copy-bucket [src dst]
  (doseq [k (bucket-keys src)]
    (bucket-put dst k (bucket-get src k))))

(defn bucket-inc [b k]
  (bucket-update
   b k
   (fnil inc 0)))

(defn bucket-merge-to!
  "merge takes (k to-value from-value)"
  [from to]
  {:pre [(or (map? from) (satisfies? IReadBucket from))
         (and (satisfies? IWriteBucket to))]}	 
  (doseq [[k v] (if (map? from) from
                    (bucket-seq from))]
    (bucket-merge to k v))
  to)

;;TODO: remove flush check, moving away from legacy api.
(defmulti bucket #(do (assert (not (contains? % :flush)))
		   (or (:type %) :mem)))

(defn with-flush
  "Takes a bucket with a merge fn and wraps with an in-memory cache that can be flushed with bucket-sync.
   Read operations read from the underlying store, and will not reflect unflushed writes."
  ([b merge-fn & {:keys [block-size]
		  :or {block-size 1000}}]
     (let [mem-bucket (bucket {:type :mem})
           do-flush! #(doseq [ks (partition-all block-size (bucket-keys mem-bucket))
			      :when ks
			      [op vs] (group-by last (map (fn [k] (cons k (bucket-delete mem-bucket k))) ks))]
			(when op
			  (case op
				:put    (bucket-batch-put b (map drop-last vs))
				:update (bucket-batch-merge b (map drop-last vs)))))]
       (reify
	IReadBucket
	(bucket-get [this k] (bucket-get b k))
	(bucket-exists? [this k] (bucket-exists? b k))
	(bucket-keys [this] (bucket-keys b))
	(bucket-count [this] (bucket-count b))
	(bucket-batch-get [this ks] (bucket-batch-get b ks))
	(bucket-seq [this] (bucket-seq b))     
	
	IMergeBucket
	(bucket-merge [this k v]
		      (bucket-update this k #(merge-fn k % v)))
	(bucket-batch-merge [this kvs]
			    (default-bucket-batch-merge this merge-fn kvs))

	IWriteBucket
	(bucket-update [this k f]
		       (bucket-update
			mem-bucket k
			(fn [old-tuple]
			  (let [[val op] (or old-tuple [nil :update])]
			    [(f val) op]))))
	(bucket-delete [this k]
		       (bucket-delete mem-bucket k)
		       (bucket-delete b k))
	(bucket-put [this k v]
		    (bucket-put mem-bucket k [v :put]))
	(bucket-batch-put [this kvs] (default-bucket-batch-put this kvs))
	(bucket-sync [this]
		     (do-flush!)
		     (bucket-sync b))
	(bucket-close [this]
		      (do-flush!)
		      (bucket-close b))))))

(defn with-cache
  ([b merge-fn]
     (let [mem-bucket (bucket {:type :mem})
           do-flush! #(doseq [[k [v op]] (bucket-seq mem-bucket)]
			(case op
			      :put    (bucket-put b k v)
			      :update (bucket-merge b k v)))]
       (reify
	IReadBucket
	(bucket-get [this k]
		    (or (let [[v op :as res] (bucket-get mem-bucket k)]
			  (case (or op :nil)
				:put v
				:nil (or res nil)
				:update (merge-fn k (bucket-get b k) v)))
			(when-let [v (bucket-get b k)]
			  (bucket-merge this k v)
			  v)))
	(bucket-exists? [this k] (or (bucket-exists? mem-bucket k) (bucket-exists? b k)))
	(bucket-keys [this] (throw (UnsupportedOperationException.)))
	(bucket-count [this] (throw (UnsupportedOperationException.)))
	(bucket-batch-get [this ks] (default-bucket-batch-get this ks))
	(bucket-seq [this] (throw (UnsupportedOperationException.)))
	
	IMergeBucket
	(bucket-merge [this k v]
		      (bucket-update this k #(merge-fn k % v)))
	(bucket-batch-merge [this kvs]
			    (bucket-batch-merge this kvs))

	IWriteBucket
	(bucket-update [this k f]
		       (bucket-update
			mem-bucket k
			(fn [old-tuple]
			  (let [[val op] (or old-tuple [nil :update])]
			    [(f val) op]))))
	(bucket-delete [this k]
		       (bucket-delete mem-bucket k)
		       (bucket-delete b k))
	(bucket-put [this k v]
		    (bucket-put mem-bucket k [v :put]))
	(bucket-batch-put [this kvs] (default-bucket-batch-put this kvs))
	(bucket-sync [this]
		     (do-flush!)
		     (bucket-sync b))
	(bucket-close [this]
		      (do-flush!)
		      (bucket-close b))))))

(defn wrapper-policy [b {:keys [merge cache?] :as args}]
  (cond
   (and merge cache?) (with-cache b merge)
   merge (with-flush b merge)
   :else b))

(defmethod bucket :fs [{:keys [name path merge] :as args}]
	   (assert-keys [:name :path] args)
	   (let [dir-path (str (file path name))
		 f (if (string? dir-path)
		     (file dir-path)
		     dir-path)]
	     (.mkdirs f)
	     (->
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
	       (bucket-count [this] (count (bucket-keys this)))
	       

	       IMergeBucket
	       (bucket-merge [this k v]
			     (default-bucket-merge this merge k v))
	       (bucket-batch-merge [this kvs]
			     (default-bucket-batch-merge this merge kvs))

	       
	       IWriteBucket
	       (bucket-put [this k v]
			   (let [f (File. f ^String(ring/url-encode k))]
			     (spit f (pr-str v))))
	       (bucket-batch-put [this kvs] (default-bucket-batch-put this kvs))
	       (bucket-delete [this k]
			      (let [f (File. f ^String (ring/url-encode  k))]
				(.delete f)))
	       (bucket-update [this k f]
			      (default-bucket-update this k f))
	       (bucket-sync [this] nil)
	       (bucket-close [this] nil))
	      (wrapper-policy args))))

(defmethod bucket :mem [{:keys [merge]}]
	   (hashmap-bucket (ConcurrentHashMap.) merge))

;;; Extend Read buckets to clojure maps

(def ^:private read-bucket-map-impls
     {:bucket-get (fn [this k] (this k))
      :bucket-seq (fn [this] (seq this))
      :bucket-batch-get (fn [this ks] (default-bucket-batch-get this ks))
      :bucket-keys (fn [this] (keys this))
      :bucket-exists? (fn [this k] (find this k))
      :bucket-count (fn [this] (count this))})

(doseq [c [clojure.lang.PersistentHashMap
	   clojure.lang.PersistentArrayMap
	   clojure.lang.PersistentStructMap]]
  (extend c IReadBucket read-bucket-map-impls))

(def streaming-ops #{:keys :seq})

(def read-ops
  {:get bucket-get
   :batch-get bucket-batch-get
   :seq bucket-seq
   :keys bucket-keys
   :count bucket-count
   :exists? bucket-exists?})

(def write-ops
     {:put bucket-put
      :batch-put bucket-batch-put
      :delete bucket-delete
      :merge bucket-merge
      :batch-merge bucket-batch-merge
      :update bucket-update
      :sync bucket-sync
      :close bucket-close
      :optimize bucket-optimize})