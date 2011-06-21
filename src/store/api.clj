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

(def bucket-ops
     {:add (fn [store bucket-name]
	     (let [bucket (create-buckets (assoc (.context store)
					    :name bucket-name))]
	       (bucket-put
		(.bucket-map store)
		bucket-name
		bucket)))
      :remove (fn [store bucket-name]
		(bucket-delete
		 (.bucket-map store) bucket-name))})

(def rest-bucket-ops
     {:add (fn [store bucket-name]
	     (rest-call (assoc (.context store)
			  :op "add"
			  :name bucket-name)))
      :remove (fn [store bucket-name]
		(rest-call (assoc (.context store)
			     :op "remove"
			     :name bucket-name)))})

;;TODO: bad semantics to return nil for all operations otehr than reads.  The right thing to do is polymorphic behavior based on type of store, but dealing with the issue at the net store level creates even worse hacks than this.  #HACK deones the hacks.
(defn store-op [store op name & args]
  (if (find bucket-ops op)
    (let [{:keys [type]} (.context store)]
      (when (= type :rest)
	((op rest-bucket-ops) store name))
      ((op bucket-ops) store name)
      nil) ;; #hack to return nil for writes
    (let [read (read-ops op)
	  spec (->> name (bucket-get (.bucket-map store)))
	  b (if read (:read spec)
		(:write spec))
	  f (or read (write-ops op))]
      (when-not b
	(let [read-or-write (if read "read" "write")]
	  (throw (Exception. (format "No %s operation for bucket %s" read-or-write name)))))
      (let [res (apply f b args)]
	(when read res))))) ;;#hack to return nil for writes

(deftype Store [bucket-map context]
  clojure.lang.IFn
  (invoke [this op bucket-name]
	  (store-op this op bucket-name))
  (invoke [this op bucket-name key]
	  (store-op this op bucket-name key))
  (invoke [this op bucket-name key val]
	  (store-op this op bucket-name key val))
  (applyTo [this args]
	   (apply store-op this args)))

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