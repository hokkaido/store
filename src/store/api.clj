(ns store.api
  (:use [plumbing.core :only [?>]]
	[clojure.java.io :only [file]]
	store.core
	store.net
	store.bdb))

(defn raw-bucket [name {:keys [type db-env host port path] :as opts}]
  (case type
	:bdb  (bdb-bucket
	       (apply bdb-db name db-env
		      (apply concat (merge {:cache-mode :evict-ln}
					   opts))))
	:fs (fs-bucket (str (file path name)))
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
  [name {:keys [type,merge,flush?] :as spec}]
  (-> (raw-bucket name spec)
      (?> merge with-merge merge)
      (?> flush? with-reading-flush)))

(defn build-buckets [specs & [context]]
  (let [specs (if (map? specs)
		(map #(if (not context)
			%
			(merge context %))
		     specs)
		(zipmap specs (repeat context)))]
    (->> specs
	 (map (fn [[name spec]]
		[name (bucket name spec)]))
	 (into {}))))

(defn flush! [buckets spec-map]
  #(doseq [[name spec] spec-map
	   :when (:flush? spec)
	   :let [b (buckets name)]]
     (bucket-sync b)))