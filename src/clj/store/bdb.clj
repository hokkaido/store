(ns store.bdb
  (:import com.sleepycat.je.Database 
	   com.sleepycat.je.DatabaseEntry
	   com.sleepycat.je.LockMode
	   com.sleepycat.je.Environment
	   com.sleepycat.je.EnvironmentConfig
	   com.sleepycat.je.DatabaseConfig
	   com.sleepycat.je.OperationStatus)
  (:use store.api))

;;http://download.oracle.com/docs/cd/E17277_02/html/GettingStartedGuide

(defn from-entry [#^DatabaseEntry e]
  (read-string (String. (.getData e) "UTF-8")))

(defn to-entry [clj]
  (DatabaseEntry. (.getBytes (pr-str clj) "UTF-8")))

(defn bdb-put [#^Database db k v]
  (let [entry-key (to-entry k)
	entry-val (to-entry v)]
    (.put db nil entry-key entry-val)))

(defn bdb-get [#^Database db k]
  (let [entry-key (to-entry k)
	entry-val (DatabaseEntry.)]
    (if (= (.get db nil entry-key entry-val LockMode/DEFAULT)
	   OperationStatus/SUCCESS)
      (from-entry entry-val))))

(defn entries-seq
 [#^Database db]
 (let [cursor (.openCursor db nil nil)]
   (take-while identity
               (repeatedly
                #(let [k (DatabaseEntry.)
                       v (DatabaseEntry.)]
                   (if (not (= (.getNext cursor k v LockMode/DEFAULT)
                               OperationStatus/SUCCESS))
                     ;; return nil
                     (do (.close cursor)
                         nil)
                     [(from-entry k)
                      (from-entry v)]))))))

(defn bdb-delete [#^Database db k]
  (let [entry-key (to-entry k)]
    (.delete db nil entry-key)))

;;http://download.oracle.com/docs/cd/E17076_02/html/java/com/sleepycat/db/EnvironmentConfig.html
;;http://doc.sumy.ua/db/db/ref/cam/intro.html

(defn bdb-open [& {:keys [env-path bucket read-only]
		   :or {read-only false
			env-path "/var/bdb/"}}]
  (let [env-config (doto (EnvironmentConfig.)
		     (.setAllowCreate true)
		     (.setReadOnly read-only))
	db-env (-> env-path java.io.File. (Environment. env-config))
	db-config (doto (DatabaseConfig.)
		    (.setAllowCreate true))]
    (.openDatabase db-env nil bucket db-config)))

(defn bdb-bucket
  "returns callback fn for a Berkeley DB backed bucket."
  [& env]
  (let [db (apply bdb-open env)]
    (reify IBucket
	   (bucket-get [this k]
		       (bdb-get db k))
	   (bucket-put [this k v]
		       (bdb-put db k v))
	   (bucket-keys [this] (default-bucket-keys this))
	   (bucket-seq [this]
		       (entries-seq db))
	   (bucket-delete [this k]
			  (bdb-delete db k))
	   (bucket-update [this k f]
			  (default-bucket-update this k f))
	   (bucket-exists? [this k] (default-bucket-exists? this k)))))