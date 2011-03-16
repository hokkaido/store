(ns store.bdb
  (:use store.api
        [clojure.java.io :only [file copy]]
	[clojure.contrib.shell :only [sh]])
  (:import [com.sleepycat.je
	    Database 
	    DatabaseEntry
	    LockMode
	    Environment
	    EnvironmentConfig
	    DatabaseConfig
	    OperationStatus
	    CheckpointConfig
	    CacheMode]
	   [com.sleepycat.je.util
	     DbBackup]))

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
                   (if (not (= (.getNext cursor k v LockMode/READ_UNCOMMITTED)
                               OperationStatus/SUCCESS))
                     ;; return nil
                     (do (.close cursor)
                         nil)
                     [(from-entry k)
                      (from-entry v)]))))))

(defn bdb-delete [^Database db k]
  (let [entry-key (to-entry k)]
    (.delete db nil entry-key)))

(defn bdb-conf [read-only-db deferred-write cache-mode]
  (let []
    (doto (DatabaseConfig.)
      (.setReadOnly read-only-db)
      (.setAllowCreate (not read-only-db))
      (.setDeferredWrite deferred-write)
      (.setCacheMode (cache-modes cache-mode)))))

;;http://download.oracle.com/docs/cd/E17277_02/html/java/com/sleepycat/je/EnvironmentConfig.html

(defn bdb-env
  "Parameters:
   :path - bdb environment path
   :read-only - set bdb environment to be read-only
   :checkpoint-kb - how many kb to write before checkpointing
   :checkpoint-mins - how many mins to wait before checkpointing
   :clean-util-thresh - % to trigger log file cleaning (higher means cleaner)
   :locking - toggle locking, if turned off then the cleaner is also
   :cache-percent - percent of heap to use for BDB cache"
  [& {:keys [path read-only
             checkpoint-kb checkpoint-mins
	     num-cleaner-threads 
             locking cache-percent clean-util-thresh
	     checkpoint-high-priority?
	     max-open-files]
      :or {read-only false
           path "/var/bdb/"
           checkpoint-kb 0
	   num-cleaner-threads 1
           checkpoint-mins 0
	   clean-util-thresh 50
	   checkpoint-high-priority? false
           locking true
           cache-percent 60
	   max-open-files 512}}]
  (let [env-config (doto (EnvironmentConfig.)
                     (.setReadOnly read-only)
                     (.setAllowCreate (not read-only))
		     (.setConfigParam (EnvironmentConfig/CLEANER_MIN_UTILIZATION)
				      (str clean-util-thresh))
		     (.setConfigParam (EnvironmentConfig/CLEANER_THREADS)
				      (str num-cleaner-threads))
		     (.setConfigParam (EnvironmentConfig/CHECKPOINTER_HIGH_PRIORITY)
				      (str checkpoint-high-priority?))
		     (.setConfigParam (EnvironmentConfig/LOG_FILE_CACHE_SIZE)
				      (str max-open-files))
                     (.setLocking locking)
                     (.setCachePercent cache-percent))]
    (doto CheckpointConfig/DEFAULT
      (.setKBytes checkpoint-kb)
      (.setMinutes checkpoint-mins))
    (Environment. (file path) env-config)))

(defn ^long bdb-env-backup
  "backup bdb environment to another location. pauses
   the addition or deletion of files. returns the id
   of the last file in the backup. needed to do continuous backups
   options
  :delete delete the existing backup
  :last-id last id of backup. pass this option in to only
  back up new files"
  [^Environment env ^String copy-path &
   {:keys [delete? last-id] :or {delete true}}]
  (when delete?
    (sh "rm" "-fr" copy-path))
  (sh "mkdir" "-p" copy-path)
  (let [backup (if last-id
		 (DbBackup. env ^long last-id)
		 (DbBackup. env))]
    (.startBackup backup)
    (doseq [path (.getLogFilesInBackupSet backup)
	    :let [src (java.io.File. (.getHome env) path)
		  dst (java.io.File. copy-path path)]]
      (copy src dst))
    (let [ret (.getLastFileInBackupSet backup)]
      (.endBackup backup)
      ret)))

(defn bdb-env-close [^Environment env] (.close env))  

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
  (reify IReadBucket
         (bucket-get [this k]
                     (bdb-get db k))
	 (bucket-batch-get [this ks]
			   (default-bucket-batch-get this ks))
         
         (bucket-keys [this] (default-bucket-keys this))
         (bucket-seq [this]
                     (entries-seq db))

	 (bucket-exists? [this k] (default-bucket-exists? this k))

	 IWriteBucket

	 (bucket-put [this k v]
                     (bdb-put db k v))
	 (bucket-delete [this k]
                        (bdb-delete db k))
         (bucket-update [this k f]
                        (default-bucket-update this k f))
         (bucket-sync [this]
		      (when (-> db .getConfig .getDeferredWrite)
			(.sync db)))
         (bucket-close [this] (.close db))))