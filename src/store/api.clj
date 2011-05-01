(ns store.api
  (:use [plumbing.core :only [?> ?>>]]
	[clojure.java.io :only [file]]
	store.core
	store.net
	store.bdb))

(defn raw-bucket [{:keys [name type db-env host port path] :as opts}]
  (case type
	:bdb  (bdb-bucket
	       (apply bdb-db name db-env
		      (apply concat (merge {:cache-mode :evict-ln}
					   opts))))
	:fs (fs-bucket path name)
	:mem (hashmap-bucket)
	:rest (rest-bucket :host host
			   :port port
			   :name name)
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

(defn to-kv [k v m]
  [(m k) (m v)])

(defn buckets [specs & [context]]
  (->> specs
       (map #(->> %
		  (?>> (string? %) hash-map :name)
		  (add-context context)
		  ((fn [m] (assoc m :bucket (bucket m))))
		  (to-kv :name :bucket)))
       (into {})))

;;TODO: change to each bucket scheduling it's own flushing on a seperate thread.
(defn flush! [buckets spec-map]
  #(doseq [[name spec] spec-map
	   :when (:flush? spec)
	   :let [b (buckets name)]]
     (bucket-sync b)))