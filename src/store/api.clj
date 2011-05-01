(ns store.api
  (:use [plumbing.core :only [?> ?>>]]
	[clojure.java.io :only [file]]
	store.core
	store.net
	store.riak
	store.bdb))

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

(defn with-reading-flush [b]
  (compose-buckets
     b
     (with-flush b)))

(defn bucket
  [{:keys [merge,flush?] :as spec}]
  (-> (raw-bucket spec)
      (?> merge with-merge merge)
      (?> flush? with-reading-flush)))

(defn add-context [context spec]
  (if (not context)
    spec
    (merge
     spec
     context
     (context (:id spec)))))

(defn to-kv [k m]
  [(m k) m])

(defn create-buckets [{:keys [read write] :as spec}]
  (let [r (if read
	    (bucket (merge spec read))
	    (bucket spec))
	w (if write
	    (bucket (merge spec write))
	    r)]
    (assoc spec :read r :write w)))

(defn buckets [specs & [context]]
  (->> specs
       (map #(->> %
		  (?>> (string? %) hash-map :name)
		  (add-context context)
		  create-buckets
		  (to-kv :name)))
       (into {})))

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

(defn shutdown [^Store store]
  (doseq [[name spec] (.bucket-map store)]
    (bucket-close (:read spec))
    (bucket-close (:write spec))))

(defn store [bucket-specs & [context]]
  (store.api.Store. (buckets bucket-specs context)))