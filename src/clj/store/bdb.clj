(ns store.bdb
  (:import com.sleepycat.je.DatabaseEntry
	   com.sleepycat.je.LockMode
	   com.sleepycat.je.Environment
	   com.sleepycat.je.EnvironmentConfig
	   com.sleepycat.je.DatabaseConfig
	   com.sleepycat.je.OperationStatus))

(defn put [db k v]
  (let [entry-key (DatabaseEntry. (.getBytes (pr-str k) "UTF-8"))
	entry-val (DatabaseEntry. (.getBytes (pr-str v) "UTF-8"))]
    (.put nil entry-key entry-val)))

(defn from-entry [e]
  (read-string (String. (.getData e) "UTF-8")))

(defn get [db k]
  (let [entry-key (DatabaseEntry. (.getBytes (pr-str k) "UTF-8"))
	entry-val (DatabaseEntry.)]
    (if (= (.get db nil entry-key entry-val LockMode/DEFAULT)
	   OperationStatus/SUCCESS)
      (from-entry entry-val))))

(defn entries-seq [db]
  (let [cursor (.openCursor db nil nil)]
    (lazy-seq ((fn [acc]
		 (let [k (DatabaseEntry.)
		       v (DatabaseEntry.)]
		   (if (not (= (.getNext cursor k v LockMode/DEFAULT)
			       OperationStatus/SUCCESS))
		     (do (.close cursor)
			 acc)
		     (recur
		      (cons [(from-entry k)
			     (from-entry v)]
			    acc)))))
		 []))))

(defn delete [db k]
  (let [entry-key (DatabaseEntry. (.getBytes (pr-str k) "UTF-8"))]
    (.delete nil entry-key)))

(defn open [env-path db-name]
  (let [env-config (EnvironmentConfig.)
	_ (.setAllowCreate env-config true)
	db-env (Environment. (java.io.File. env-path) env-config)
	db-config (DatabaseConfig.)
	_ (.setAllowCreate db-config true)
	db (.openDatabase db-env nil db-name db-config)]
    db))

;; (defn bdb-bucket
;;   "returns callback fn for a Berkeley DB backed bucket."
;;   ([^String bucket 
;;     {:keys [host,port,timeout,pool-timeout,retry-count,num-clients]
;;      :as spec}]
;;      (let [jedis-pool (atom nil)]
;;        (reify IBucket
;; 	      (bucket-get [this k]
;; 		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
;; 		  (with-jedis-client @jedis-pool  pool-timeout retry-count
;; 		    (fn [^Jedis c]
;; 		      (when-let [v (.get c (mk-key bucket k))]
;; 		       (read-string v)))))

;; 	      (bucket-put [this k v]
;; 		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
;; 		  (with-jedis-client @jedis-pool  pool-timeout retry-count
;; 		    (fn [^Jedis c] (.set c (mk-key bucket k) (pr-str v)))))
	      
;; 	      (bucket-keys [this]
;; 		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
;; 		  (with-jedis-client @jedis-pool pool-timeout retry-count
;; 		    (fn [^Jedis c]
;; 		      (let [prefix-ln (inc (.length bucket))]
;; 			(doall (map #(.substring ^String % prefix-ln)
;; 				    (.keys c (format "%s:*" bucket))))))))

;; 	      (bucket-seq [this] (default-bucket-seq this))
	      	      
;; 	      (bucket-delete [this k]
;; 		  (ensure-jedis-pool jedis-pool host port timeout num-clients)	  
;; 		  (with-jedis-client @jedis-pool pool-timeout retry-count
;; 		    (fn [^Jedis c] (.del c (into-array [(mk-key bucket k)])))))
	      
;; 	      (bucket-exists? [this k]
;; 		  (ensure-jedis-pool jedis-pool host port timeout num-clients)
;; 		  (default-bucket-exists? this k)
;; 		  #_(with-jedis-client @jedis-pool pool-timeout retry-count
;; 		    (fn [^Jedis c] (.exists c (mk-key bucket k)))))))))
