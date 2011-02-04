(ns store.bdb
  (:import (com.sleepycat.je Database 
                             DatabaseEntry
                             LockMode
                             Environment
                             EnvironmentConfig
                             DatabaseConfig
                             OperationStatus
                             CheckpointConfig
                             CacheMode))
  (:use store.api
        [clojure.java.io :only [file]]))

;;http://download.oracle.com/docs/cd/E17277_02/html/GettingStartedGuide

(def cache-modes {:default CacheMode/DEFAULT
                  :evict-bin CacheMode/EVICT_BIN
                  :evict-ln CacheMode/EVICT_LN
                  :keep-hot CacheMode/KEEP_HOT
                  :make-cold CacheMode/MAKE_COLD
                  :unchanged CacheMode/UNCHANGED})

(defn from-entry [^DatabaseEntry e]
  (read-string (String. (.getData e) "UTF-8")))

(defn to-entry [clj]
  (DatabaseEntry. (.getBytes (pr-str clj) "UTF-8")))

(defn bdb-put [^Database db k v]
  (let [entry-key (to-entry k)
	entry-val (to-entry v)]
    (.put db nil entry-key entry-val)))

(defn bdb-get [^Database db k]
  (let [entry-key (to-entry k)
	entry-val (DatabaseEntry.)]
    (if (= (.get db nil entry-key entry-val LockMode/DEFAULT)
	   OperationStatus/SUCCESS)
      (from-entry entry-val))))

(defn entries-seq
 [^Database db]
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
;;http://download.oracle.com/docs/cd/E17076_02/html/java/com/sleepycat/db/EnvironmentConfig.html

(defn bdb-conf [read-only-db deferred-write cache-mode]
  (let []
    (doto (DatabaseConfig.)
      (.setReadOnly read-only-db)
      (.setAllowCreate (not read-only-db))
      (.setDeferredWrite deferred-write)
      (.setCacheMode (cache-modes cache-mode)))))

(defn bdb-env
  "Parameters:
   :path - bdb environment path
   :read-only - set bdb environment to be read-only
   :checkpoint-kb - how many kb to write before checkpointing
   :checkpoint-mins - how many mins to wait before checkpointing
   :locking - toggle locking, if turned off then the cleaner is also
   :cache-percent - percent of heap to use for BDB cache"
  [& {:keys [path read-only
             checkpoint-kb checkpoint-mins
             locking cache-percent]
      :or {read-only false
           path "/var/bdb/"
           checkpoint-kb 0 
           checkpoint-mins 0
           locking true
           cache-percent 60}}]
  (let [env-config (doto (EnvironmentConfig.)
                     (.setReadOnly read-only)
                     (.setAllowCreate (not read-only))
                     (.setLocking locking)
                     (.setCachePercent cache-percent))]
    (doto CheckpointConfig/DEFAULT
      (.setKBytes checkpoint-kb)
      (.setMinutes checkpoint-mins))
    (Environment. (file path) env-config)))

(defn bdb-db
  "Parameters:
   name - database name
   :env - the database environment
   :read-only - set db to read-only, overrides environment config
   :deferred-write - toggle deferred writing to filesystem
   :cache-mode - eviction policy for cache"
  [name env & {:keys [read-only deferred-write cache-mode]
               :or {read-only false
                    deferred-write false
                    cache-mode :default}}]
  (let [db-conf (bdb-conf read-only deferred-write cache-mode)]
    (.openDatabase env nil name db-conf)))

(defn bdb-bucket
  "returns callback fn for a Berkeley DB backed bucket."
  [^Database db]
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
         (bucket-exists? [this k] (default-bucket-exists? this k))
         (bucket-sync [this] (.sync db))
         (bucket-close [this] (.close db))))