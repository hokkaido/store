(ns store.api
  (:use [plumbing.core :only [?>]]
	store.core
	store.net
	store.bdb))

(defn raw-bucket [name {:keys [type db-env host port path] :as opts}]
  (case type
	:bdb  (bdb-bucket
	       (apply bdb-db name db-env
		      (apply concat (merge {:cache-mode :evict-ln}
					   opts))))
	:fs (fs-bucket path)
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

(defn build-buckets [spec-map context]
  (->> spec-map 
       (map (fn [[name spec]]
	      [name (bucket name (merge context spec))]))
       (into {})))

(defn flush! [buckets spec-map]
  #(doseq [[name spec] spec-map
	   :when (:flush? spec)
	   :let [b (buckets name)]]
     (bucket-sync b)))