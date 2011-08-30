(ns store.core
  (:use plumbing.core
	[plumbing.error :only [assert-keys]]
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
  (bucket-batch-get [this ks] "return seq of [k v] pairs")
  (bucket-exists? [this k] "does key-value pair exists")
  (bucket-keys [this] "seq of existing keys")
  (bucket-seq [this] "seq of [k v] elems")
  (bucket-count [this] "the number of kv pairs in this bucket"))

(defprotocol IWriteBucket
  (bucket-put [this k v]
              "write value for key. return value can be anything")
  (bucket-delete [this k] "remove key-value pair")
  (bucket-update [this k f])
  (bucket-sync [this])
  (bucket-close [this]))

(defprotocol IMergeBucket
  (bucket-merge [this k v] "merge v into current value"))

(defprotocol IOptimizeBucket
  (bucket-optimize [this] "optimize for in order reads from disk on :keys and :seq requests"))
  
;;; Default Bucket Operations
(defn default-bucket-exists? [b k]
  (find-first
   (partial = k)
   (bucket-keys b)))

(defn default-bucket-batch-get [b ks]
  (for [k ks] [k (bucket-get b k)]))

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
  ([b merge-fn]
     (let [mem-bucket (bucket {:type :mem})
           do-flush! #(doseq [k (bucket-keys mem-bucket)]
			(let [[v op] (bucket-delete mem-bucket k)]
			  (case op
				:put    (bucket-put b k v)
				:update (bucket-merge b k v))))]
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

	(bucket-sync [this]
		     (do-flush!)
		     (bucket-sync b))
	(bucket-close [this]
		      (do-flush!)
		      (bucket-close b))))))


(defmethod bucket :fs [{:keys [name path merge] :as args}]
	   (assert-keys [:name :path] args)
	   (let [dir-path (str (file path name))
		 f (if (string? dir-path)
		     (file dir-path)
		     dir-path)]
	     (.mkdirs f)
	     (?>
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
	       (bucket-close [this] nil))
	      merge with-flush merge)))

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
      :delete bucket-delete
      :merge bucket-merge
      :update bucket-update
      :sync bucket-sync
      :close bucket-close
      :optimize bucket-optimize})
