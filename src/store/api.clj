(ns store.api
  (:use [plumbing.core :only [?> ?>> map-from-vals map-map]]
	[clojure.java.io :only [file]]
	[plumbing.error :only [with-ex logger]]
	store.core
	store.net
	store.bdb
        store.s3
        )
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
  [{:keys [merge,flush] :as spec}]
  (-> (raw-bucket spec)
      (?> merge with-merge merge)
      ;;WARNING!!!
      ;;super ghetto skullfuck.
      ;;need to add flush fn as merge fn and flush fn 
      (?> flush with-merge flush)
      (?> flush add-flush flush)))

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

(deftype Store [bucket-map context]
  clojure.lang.IFn
  (invoke [this op bucket-name]
	  (cond (= op :add)
		(bucket-put bucket-map bucket-name
			    (create-buckets (assoc context
					      :name bucket-name)))
		(= op :remove)
		(bucket-delete bucket-map bucket-name)
		:else (store-op bucket-map op bucket-name)))
  (invoke [this op bucket-name key]
	  (store-op bucket-map op bucket-name key))
  (invoke [this op bucket-name key val]
	  (store-op bucket-map  op bucket-name key val))
  (applyTo [this args]
	   (apply store-op bucket-map args)))

(defn flush! [^Store store]
  (doseq [[_ spec] (.bucket-map store)
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
  (-> (buckets bucket-specs context)
      start-flush-pools
      (Store. context)))