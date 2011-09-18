(ns store.core
  (:refer-clojure :exclude [get put keys seq count sync update merge])
  (:require [clojure.core :as clj])
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
  (get [this k] "fetch value for key")
  (batch-get [this ks] "return seq of [k v] pairs")
  (exists? [this k] "does key-value pair exists")
  (keys [this] "seq of existing keys")
  (seq [this] "seq of [k v] elems")
  (count [this] "the number of kv pairs in this bucket"))

(defprotocol IWriteBucket
  (put [this k v]
              "write value for key. return value can be anything")
  (batch-put [this kvs] "put a seq of [k v] pairs")
  (delete [this k] "remove key-value pair")
  (update [this k f])
  (sync [this])
  (close [this]))

(defprotocol IMergeBucket
  (merge [this k v] "merge v into current value")
  (batch-merge [this kvs] "merge key valye pairs kvs into current values"))

(defprotocol IOptimizeBucket
  (optimize [this] "optimize for in order reads from disk on :keys and :seq requests"))
  
;;; Default Bucket Operations
(defn default-exists? [b k]
  (find-first
   (partial = k)
   (keys b)))

(defn default-batch-put [b kvs]
  (doseq [[k v] kvs] (put b k v)))

(defn default-batch-get [b ks]
  (for [k ks] [k (get b k)]))

;;TODO: put on the protocol, implementations can be much more efficient deleting with cursor.
(defn clear [b]
  (doseq [k (keys b)]
    (delete b k)))

(defn default-update [b k f]
  (->>  k
        (get b)
        f
        (put b k)))

(defn default-seq [b]
  (for [k (keys b)]
    [k (get b k)]))

(defn default-merge [b merge-fn k v]
  (assert merge-fn)
  (update b k (fn [v-to] (merge-fn k v-to v))))

(defn default-batch-merge [b merge-fn kvs]
  (assert merge-fn)
  (doseq [[k v] kvs]
    (update b k (fn [v-to] (merge-fn k v-to v)))))

(defn hashmap-bucket [^ConcurrentHashMap h & [merge-fn]]
  (reify IReadBucket
	   (keys [this]
			(enumeration-seq (.keys h)))
	   (get [this k]
		       (.get h k))
	   (batch-get [this ks] (default-batch-get this ks))
	   (seq [this]
		       (for [^java.util.Map$Entry e
			     (.entrySet h)]
			 [(.getKey e) (.getValue e)]))
	   (exists? [this k] (.containsKey h k))
	   (count [this] (.size h))

	   IMergeBucket
	   (merge [this k v]
			 (default-merge this merge-fn k v))
	   (batch-merge [this kvs]
			 (default-batch-merge this merge-fn kvs))

	   IWriteBucket
	   (put [this k v]
		       (.put h k v))
	   (batch-put [this kvs] (default-batch-put this kvs))
	   (delete [this k]
			  (.remove h k))
	   (update [this k f]
			  (loop []
			    (let [v (.get h k) new-v (f v)			
				  replaced? (cond
					     (nil? v) (nil? (.putIfAbsent h k new-v))
					     (nil? new-v) (or (nil? v) (.remove h k v))
					     :else (.replace h k v new-v))]
			      (when (not replaced?)
				(recur)))))
	   (sync [this] nil)
	   (close [this] nil)))

(defn copy-bucket [src dst]
  (doseq [k (keys src)]
    (put dst k (get src k))))

(defn bucket-inc [b k]
  (update
   b k
   (fnil inc 0)))

(defn merge-to!
  "merge takes (k to-value from-value)"
  [from to]
  {:pre [(or (map? from) (satisfies? IReadBucket from))
         (and (satisfies? IWriteBucket to))]}	 
  (doseq [[k v] (if (map? from) from
                    (seq from))]
    (merge to k v))
  to)

;;TODO: remove flush check, moving away from legacy api.
(defmulti bucket #(do (assert (not (contains? % :flush)))
		   (or (:type %) :mem)))

(defn write-blocks! [writes b block-size]
  (doseq [blocks (partition-all block-size writes)
	  :when blocks
	  [op vs] (group-by first blocks)]
    (when op
      (case op
	    :put    (batch-put b (map second vs))
	    :update (batch-merge b (map second vs))))))

(defn drain-seq [b]
  (->> (keys b)
       (map (fn [k] (let [[v op] (delete b k)]
		      [op [k v]])))))

(defn checkpoint-seq [mem-bucket]
  (->> (seq mem-bucket)
       (map (fn [[k [v op]]] [op [k v]]))))

(defn with-flush
  "Takes a bucket with a merge fn and wraps with an in-memory cache that can be flushed with sync.
   Read operations read from the underlying store, and will not reflect unflushed writes."
  ([b merge-fn & {:keys [block-size]
		  :or {block-size 100}}]
     (let [mem-bucket (bucket {:type :mem})]
       (reify
	IReadBucket
	(get [this k] (get b k))
	(exists? [this k] (exists? b k))
	(keys [this] (keys b))
	(count [this] (count b))
	(batch-get [this ks] (batch-get b ks))
	(seq [this] (seq b))     
	
	IMergeBucket
	(merge [this k v]
		      (update this k #(merge-fn k % v)))
	(batch-merge [this kvs]
			    (default-batch-merge this merge-fn kvs))

	IWriteBucket
	(update [this k f]
		       (update
			mem-bucket k
			(fn [old-tuple]
			  (let [[val op] (or old-tuple [nil :update])]
			    [(f val) op]))))
	(delete [this k]
		       (delete mem-bucket k)
		       (delete b k))
	(put [this k v]
		    (put mem-bucket k [v :put]))
	(batch-put [this kvs] (default-batch-put this kvs))
	(sync [this]
		     (write-blocks! (drain-seq mem-bucket) b block-size)
		     (sync b))
	(close [this]
		      (write-blocks! (drain-seq mem-bucket) b block-size)
		      (close b))))))

(defn with-cache
  [b merge-fn  & {:keys [block-size]
		  :or {block-size 100}}]
  (let [mem-bucket (bucket {:type :mem})]
    (reify
     IReadBucket
     (get [this k]
	  (or (when-let [[v op] (get mem-bucket k)]
		v)
	      (when-let [v (get b k)]
		(merge this k v)
		v)))
     (exists? [this k] (or (exists? mem-bucket k) (exists? b k)))
     (keys [this] (throw (UnsupportedOperationException.)))
     (count [this] (throw (UnsupportedOperationException.)))
     (batch-get [this ks] (default-batch-get this ks))
     (seq [this] (throw (UnsupportedOperationException.)))
     
     IMergeBucket
     (merge [this k v]
	    (update this k #(merge-fn k % v)))
     (batch-merge [this kvs]
		  (batch-merge this kvs))

     IWriteBucket
     (update [this k f]
	     (update
	      mem-bucket k
	      (fn [old-tuple]
		(let [[val op] (or old-tuple [(get b k) :put])]
		  [(f val) op]))))
     (delete [this k]
	     (delete mem-bucket k)
	     (delete b k))
     (put [this k v]
	  (put mem-bucket k [v :put]))
     (batch-put [this kvs] (default-batch-put this kvs))
     (sync [this]
	   (write-blocks! (checkpoint-seq mem-bucket) b block-size)
	   (sync b))
     (close [this]
	    (write-blocks! (checkpoint-seq mem-bucket) b block-size)
	    (close b)))))

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
	       (get [this k]
			   (let [f (File. f ^String (ring/url-encode k))]
			     (when (.exists f) (-> f slurp read-string))))
	       (batch-get [this ks] (default-batch-get this ks))
	       (seq [this] (default-seq this))
	       (exists? [this k]
			       (let [f (File. f ^String (ring/url-encode k))]
				 (.exists f)))
	       (keys [this]
			    (for [^File c (.listFiles f)
				  :when (and (.isFile c) (not (.isHidden c)))]
			      (ring/url-decode (.getName c))))
	       (count [this] (count (keys this)))
	       

	       IMergeBucket
	       (merge [this k v]
			     (default-merge this merge k v))
	       (batch-merge [this kvs]
			     (default-batch-merge this merge kvs))

	       
	       IWriteBucket
	       (put [this k v]
			   (let [f (File. f ^String(ring/url-encode k))]
			     (spit f (pr-str v))))
	       (batch-put [this kvs] (default-batch-put this kvs))
	       (delete [this k]
			      (let [f (File. f ^String (ring/url-encode  k))]
				(.delete f)))
	       (update [this k f]
			      (default-update this k f))
	       (sync [this] nil)
	       (close [this] nil))
	      (wrapper-policy args))))

(defmethod bucket :mem [{:keys [merge]}]
	   (hashmap-bucket (ConcurrentHashMap.) merge))

;;; Extend Read buckets to clojure maps

(def ^:private read-bucket-map-impls
     {:get (fn [this k] (this k))
      :seq (fn [this] (clj/seq this))
      :batch-get (fn [this ks] (default-batch-get this ks))
      :keys (fn [this] (clj/keys this))
      :exists? (fn [this k] (find this k))
      :count (fn [this] (clj/count this))})

(doseq [c [clojure.lang.PersistentHashMap
	   clojure.lang.PersistentArrayMap
	   clojure.lang.PersistentStructMap]]
  (extend c IReadBucket read-bucket-map-impls))

(def streaming-ops #{:keys :seq})

(def read-ops
  {:get get
   :batch-get batch-get
   :seq seq
   :keys keys
   :count count
   :exists? exists?})

(def write-ops
     {:put put
      :batch-put batch-put
      :delete delete
      :merge merge
      :batch-merge batch-merge
      :update update
      :sync sync
      :close close
      :optimize optimize})

(defn dispatch [buckets op name args]
  (let [read (read-ops op)
	f (or read (write-ops op))
	spec (get buckets name)
	b (if read (:read spec)
	      (:write spec))]
    (when-not b
      (when-not spec
	(throw (Exception. (format "No bucket %s" name))))
      (let [read-or-write (if read "read" "write")]
	(throw (Exception. (format "No %s operation for bucket %s" read-or-write name)))))
    (apply f b args)))

(defn create-buckets [{:keys [read write] :as spec}]
  (let [r (bucket (if read (clj/merge spec read) spec))
	w (if write
	    (bucket (clj/merge spec write))
	    r)]
    (assoc spec
      :read r :write w      
      :write-spec (or write spec))))

(defn buckets [specs & [context]]
  (->> specs
       (map #(->> %
		  (?>> (string? %) hash-map :name)
		  (clj/merge context)
		  create-buckets
		  ((fn [m] [(:name m) m]))))
       (into {})
       (ConcurrentHashMap.)
       hashmap-bucket))
