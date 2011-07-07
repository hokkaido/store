(ns store.api
  (:use [plumbing.core :only [?> ?>> map-from-vals map-map]]
	[clojure.java.io :only [file]]
	[plumbing.error :only [with-ex logger]]
	store.core
	store.net
	store.bdb
        store.s3)
  (:import [java.util.concurrent Executors TimeUnit
	    ConcurrentHashMap]))

(defn raw-bucket [{:keys [name type db-env host port path prefix]
		   :or {type :mem}
		   :as opts}]
  (case type
	:bdb  (bdb-bucket
	       (apply bdb-db name db-env
		      (apply concat (merge {:cache-mode :evict-ln}
					   opts))))
	:fs (fs-bucket path name)
	:mem (hashmap-bucket)
	:rest (apply rest-bucket (apply concat opts))
        :s3   (s3-bucket (s3-connection opts) (str prefix name))
	(throw (java.lang.Exception.
		(format "bucket type %s does not exist." type)))))

(defn add-flush [bucket flush]
  (compose-buckets
   bucket
   (with-flush bucket flush)))

(defn bucket
  [{:keys [merge,flush,type] :as spec}]
  (-> (raw-bucket spec)
      (?> (and merge (not= :rest type)) with-merge merge)
      (?> flush add-flush merge)))

(defn add-context [context spec]
  (if-not context
    spec
    (merge
     context
     (context (:id spec))
     spec)))

(defn create-buckets [{:keys [read write] :as spec}]
  (let [r (if read
	    (bucket (merge spec read))
	    (bucket spec))
	w (if write
	    (bucket (merge spec write))
	    r)]
    (assoc spec
      :read r :write w      
      :write-spec (or write spec))))

(defn to-kv [f m]
  [(f m) m])

(defn buckets [specs & [context]]
  (->> specs
       (map #(->> %
		  (?>> (string? %) hash-map :name)
		  (add-context context)
		  create-buckets
		  (to-kv :name)))
       (into {})
       (ConcurrentHashMap.)
       hashmap-bucket))

(def bucket-ops
     {:buckets (fn [store name]  ;;HACK, don't need name.  just puinting until we do api overahul.
		(bucket-keys (.bucket-map store)))
      :bucket (fn [store bucket-name]
		(bucket-get (.bucket-map store) bucket-name))
      :add (fn [store bucket-name]
	     (let [bucket (create-buckets (assoc (.context store)
					    :name bucket-name))]
	       (bucket-put
		(.bucket-map store)
		bucket-name
		bucket)))
      :remove (fn [store bucket-name]
		(bucket-delete
		 (.bucket-map store) bucket-name))})

(defn store-op [store op & args]
  (let [name (first args)
	args (rest args)]
    (if (find bucket-ops op)
      (let [{:keys [type]} (.context store)
	    local ((op bucket-ops) store name)]
	(if-not (= type :rest) local
		((op rest-bucket-ops) store name)))
      (let [read (read-ops op)
	    spec (->> name (bucket-get (.bucket-map store)))
	    b (if read (:read spec)
		  (:write spec))
	    f (or read (write-ops op))]
	(when-not b
	  (when-not spec
	    (throw (Exception. (format "No bucket %s" name))))
	  (let [read-or-write (if read "read" "write")]
	    (throw (Exception. (format "No %s operation for bucket %s" read-or-write name)))))
	(apply f b args)))))

(deftype Store [bucket-map context]
  clojure.lang.IFn
   (invoke [this op]
	  (store-op this op nil))
  (invoke [this op bucket-name]
	  (store-op this op bucket-name))
  (invoke [this op bucket-name key]
	  (store-op this op bucket-name key))
  (invoke [this op bucket-name key val]
	  (store-op this op bucket-name key val))
  (applyTo [this args]
	   (apply store-op this args)))

(defn add-bucket [^Store s bucket-name bucket]
  (bucket-put  (.bucket-map s)
	       bucket-name
	       {:read bucket
		:write bucket})
  s)

(defn flush! [^Store store]
  (doseq [[_ spec] (bucket-seq (.bucket-map store))
	  :when (-> spec :write-spec :flush)]
    (bucket-sync (:write spec))))

(defn shutdown [^Store store]
  (doseq [[name spec] (bucket-seq (.bucket-map store))
	  f (:shutdown spec)]
    (with-ex (logger) f))
  (doseq [[name spec] (bucket-seq (.bucket-map store))]
    (with-ex (logger)  bucket-close (:read spec))
    (with-ex (logger)  bucket-close (:write spec))))

(defn start-flush-pools [bucket-map]
  (->> bucket-map
       bucket-seq
       (map-map
	(fn [{:keys [write,write-spec] :as bucket-spec}]
	  (let [{:keys [flush-freq]} write-spec]
	    (if-not flush-freq
	      bucket-spec
	      (let [pool (doto (Executors/newSingleThreadScheduledExecutor)
			   (.scheduleAtFixedRate			  
			    #(with-ex (logger) bucket-sync write)
			    (long 0) (long flush-freq)
			    TimeUnit/SECONDS))]
		(-> bucket-spec
		    (assoc :flush-pool pool)
		    (update-in [:shutdown]
			       conj
			       (fn []
				 (bucket-sync write)
				 (.shutdownNow pool)))))))))
   
       doall
       (into {})
       (ConcurrentHashMap.)
       hashmap-bucket))

(defn store [bucket-specs & [context]]
  (let [context (or context {})]
    (-> (buckets bucket-specs context)
	start-flush-pools
	(Store. context))))

(defn clone [s & [context]]
  (store (bucket-keys (.bucket-map s)) context))

(defn mirror-remote [spec]
  (let [s (store [] spec)
	ks (s :buckets)]
    (store ks spec)))