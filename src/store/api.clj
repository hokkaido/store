(ns store.api
  (:use [plumbing.core :only [?> ?>> map-from-vals map-map]]
	[clojure.java.io :only [file]]
	[plumbing.error :only [with-ex logger]]
	store.core
	store.net
	store.riak
	store.bdb)
  (:import [java.util.concurrent Executors TimeUnit]))

(defn raw-bucket [{:keys [name type db-env host port path]
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
	:riak (apply riak-bucket (apply concat opts)) 
	(throw (java.lang.Exception.
		(format "bucket type %s does not exist." type)))))

(defn with-reading-flush 
  ([b] (with-reading-flush b [b]))
  ([b flushes]
   (compose-buckets b (with-flush flushes))))

(defn bucket
  [{:keys [merge,flush?] :as spec}]
  (-> (raw-bucket spec)
      (?> merge with-merge merge)))

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

(defn add-flush-listeners [bucket-map bucket-spec]
  (let [b (-> bucket-spec :write)
	flush (-> bucket-spec :write-spec :flush)]
    (if (or (empty? flush))
      bucket-spec
      (update-in bucket-spec [:write]
	with-reading-flush
	(map (fn [f]
	       (if (= f :self)
		 b
		 (:write (bucket-map f))))
	     flush)))))

(defn with-flushes [bucket-map]  
  (map-map
   (partial add-flush-listeners bucket-map)
   bucket-map))

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
       with-flushes))

(deftype Store [bucket-map]
  clojure.lang.IFn
  (invoke [this op bucket-name]
	  (store-op bucket-map op bucket-name))
  (invoke [this op bucket-name key]
	  (store-op bucket-map op bucket-name key))
  (invoke [this op bucket-name key val]
	  (store-op bucket-map  op bucket-name key val))
  (applyTo [this args]
	   (apply store-op bucket-map args)))

(defn flush! [^Store store]
  (doseq [[_ spec] (.bucket-map store)
	  :when (-> spec :write-spec :flush empty? not)]
    (bucket-sync (:write spec))))

(defn shutdown [^Store store]
  (doseq [[name spec] (.bucket-map store)
	  f (:shutdown spec)]
    (with-ex (logger) f))
  (doseq [[name spec] (.bucket-map store)]
    (with-ex (logger)  bucket-close (:read spec))
    (with-ex (logger)  bucket-close (:write spec))))

(defn start-flush-pools [bucket-map]
  (->> bucket-map
       (map-map
	(fn [{:keys [write,write-spec] :as bucket-spec}]
	  (let [{:keys [flush,flush-freq]} write-spec]
	    (if (or (empty? flush) (nil? flush-freq))
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
   
       doall))

(defn store [bucket-specs & [context]]
  (-> (buckets bucket-specs context)
      start-flush-pools
      (Store.)))