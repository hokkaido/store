(ns store.bdb
  (:use store.core
        [clojure.java.io :only [file copy make-parents]]
        [clojure.contrib.shell :only [sh]]
	[plumbing.core :only [?>]]
	[plumbing.error :only [assert-keys]])
  (:require [clj-json.core :as json]
	    [clojure.contrib.logging :as log]
            [plumbing.observer :as obs])
  (:import (com.sleepycat.je Database 
                             DatabaseEntry
                             LockMode
                             Environment
                             EnvironmentConfig
                             DatabaseConfig
                             Cursor
			     CursorConfig
                             OperationStatus
                             CheckpointConfig
                             CacheMode)
           (com.sleepycat.je.util DbBackup)
           [java.util.concurrent LinkedBlockingQueue]))

;;http://download.oracle.com/docs/cd/E17277_02/html/GettingStartedGuide

(defn- megs [x] (* x 1000000))
(defn- seconds [x] (* x 1000))

(def cache-modes {:default CacheMode/DEFAULT
                  :evict-bin CacheMode/EVICT_BIN
                  :evict-ln CacheMode/EVICT_LN
                  :keep-hot CacheMode/KEEP_HOT
                  :make-cold CacheMode/MAKE_COLD
                  :unchanged CacheMode/UNCHANGED})

(defn to-str [clj]
  (.getBytes (pr-str clj) "UTF-8"))

(defn from-str [data]
  (when data
    (read-string (String. data "UTF-8"))))

(defn from-snappy [data]
  (when data
    (json/deserialize-snappy data)))

(defn to-snappy [clj]
  (json/serialize-snappy clj))

(defn advance [^Cursor cursor]
  (let [k (DatabaseEntry.)
        v (doto (DatabaseEntry.)
	    (.setPartial 0 0 true))]	
    (.getNext cursor k v LockMode/READ_UNCOMMITTED)
    cursor))

(defn cursor-next
  "returns a fn which acts as a cursor over db. each call
   returns a [key value] pair. closes cursor when all entries exhausted"
  [^Cursor cursor keys-only deserialize]
  (let [k (DatabaseEntry.)
        v (if keys-only
            (doto (DatabaseEntry.)
              (.setPartial 0 0 true))
            (DatabaseEntry.))]	
    (if (not (= (.getNext cursor k v LockMode/READ_UNCOMMITTED)
                OperationStatus/SUCCESS))
      ;; return nil
      (do (.close cursor)
          nil)
      (if keys-only
	(deserialize (.getData k))
	[(deserialize (.getData k))
	 (deserialize (.getData v))]))))

(defn seque3 [observer n seq-fn]
  (let [q (LinkedBlockingQueue. (int n))
        NIL (Object.)   ;nil sentinel since LBQ doesn't support nils
        done? (atom false)]
    (obs/watch-fn!
      observer :queue-size
      (fn [^LinkedBlockingQueue q done?]
        (if @done? obs/*stop-watching* {:count 1 :size (.size q)}))
      q done?)
    (future
      (try
        (loop [s (seq  (seq-fn))]
          (if s
            (do (let [x (first s)] (.put q (if (nil? x) NIL x)))
                (recur (next s)))
            (.put q q))) ; q itself is eos sentinel
        (catch Exception e
          (.put q q)
          (throw e))))
    
    ((fn drain []
       (lazy-seq
        (let [x (.take q)]
          (if (identical? x q)    ;q itself is eos sentinel
            (do (reset! done? true) nil)
            (cons (if (identical? x NIL) nil x) (drain)))))))))

(defprotocol PCloser
  (justClosed [this]))

(deftype Closer [^Cursor cursor ^{:unsynchronized-mutable true} closed?]
  Object
  (finalize [this]
    (when-not closed?
      (log/info (str "Warning: leaked BDB cursor " cursor this))
      (.close cursor)))

  PCloser
  (justClosed [this] (set! closed? true)))

(defn cursor-seq [^Database db deserialize observer & {:keys [keys-only]
                                           :or {keys-only false}}]
  (seque3 observer 64 
          #(let [cursor (.openCursor db nil (doto (CursorConfig.) (.setReadUncommitted true)))
                 dummy (Closer. cursor false)]             
             ((fn make-seq [dummy]
                (lazy-seq 
                 (if-let [x (cursor-next cursor keys-only deserialize)]
                   (cons x (make-seq dummy))
                   (do (justClosed dummy) nil))))
            dummy))))

(defn optimize-db
  "Walk over the database and rewrite each record, so that subsequent traversals
   will be in sequential order on disk"
  [^Database db]
  (let [cursor (.openCursor db nil (doto (CursorConfig.) (.setReadUncommitted true)))
	k (DatabaseEntry.)
	v (DatabaseEntry.)]
    (while (= (.getNext cursor k v LockMode/READ_UNCOMMITTED)
	      OperationStatus/SUCCESS)
      (.putCurrent cursor v))
    (.close cursor)))

(defn ^DatabaseConfig bdb-conf [read-only-db deferred-write cache-mode]
  (doto (DatabaseConfig.)
    (.setReadOnly read-only-db)
    (.setAllowCreate (not read-only-db))
    (.setDeferredWrite deferred-write)
    (.setCacheMode (cache-modes cache-mode))))

(defn- ^Environment bdb-env
  "Parameters:
   :path - bdb environment path
   :read-only - set bdb environment to be read-only
   :checkpoint-kb - how many kb to write before checkpointing
   :checkpoint-mins - how many mins to wait before checkpointing
   :clean-util-thresh - % to trigger log file cleaning (higher means cleaner)
   :locking - toggle locking, if turned off then the cleaner is also
   :cache-percent - percent of heap to use for BDB cache"
  [{:keys [path
             read-only
             checkpoint-kb  ;;corresponds to new
             checkpoint-mins ;;corresponda to new
             num-cleaner-threads 
             locking
             lock-timeout
             cache-size ;;new
             clean-util-thresh
             checkpoint-high-priority?
             checkpoint-bytes-interval
             max-open-files
             min-file-utilization
             checkpoint-wakeup-interval
             log-file-max]
      :or {read-only false
           path "/var/bdb/"
           checkpoint-kb 0
           num-cleaner-threads 3
           checkpoint-mins 0
           clean-util-thresh 75
           min-file-utilization 5
           checkpoint-high-priority? true
           checkpoint-bytes-interval (megs 5)
           checkpoint-wakeup-interval (seconds 30000) ;;in microseconds
           locking true
           lock-timeout 500 ;;new
           cache-size 512 ;;new -- based on % of heap give it about 2/3 of heap up to like 10 ... cap at 10 GB above
           max-open-files 512
           log-file-max (megs 64)}}]
  (sh "mkdir" "-p" path)
  (let [env-config (doto (EnvironmentConfig.)
                     (.setReadOnly read-only)
                     (.setAllowCreate (not read-only))
                     (.setConfigParam (EnvironmentConfig/CLEANER_MIN_UTILIZATION)
                                      (str clean-util-thresh))
                     (.setConfigParam (EnvironmentConfig/CLEANER_MIN_FILE_UTILIZATION)
                                      (str min-file-utilization))
                     (.setConfigParam (EnvironmentConfig/CLEANER_THREADS)
                                      (str num-cleaner-threads))
                     (.setConfigParam (EnvironmentConfig/CHECKPOINTER_HIGH_PRIORITY)
                                      (str checkpoint-high-priority?))
                     (.setConfigParam (EnvironmentConfig/CHECKPOINTER_WAKEUP_INTERVAL)
                                      (str checkpoint-wakeup-interval)) ;;new
                     (.setConfigParam (EnvironmentConfig/CHECKPOINTER_BYTES_INTERVAL)
                                      (str checkpoint-bytes-interval)) ;;new
                     (.setConfigParam (EnvironmentConfig/LOG_FILE_CACHE_SIZE)
                                      (str max-open-files))
                     (.setConfigParam (EnvironmentConfig/LOG_FILE_MAX)  ;;new
                                      (str log-file-max))
                     (.setLocking locking)
                     (.setCacheSize (megs cache-size)))]
    (make-parents (file path "touch"))
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

(defmethod bucket :bdb
  [{:keys [^String name path cache read-only-env cache-mode read-only deferred-write merge
           observer serialize deserialize]
    :or {cache-mode :evict-ln
	 read-only false	 
	 deferred-write false
	 serialize to-snappy
	 deserialize from-snappy}
    :as args}]
  (assert (not (contains? args :cache)))  ;;TOOD: remove later.  notifying clients of their broken api call.
  (assert-keys [:name :path] args)
  (let [observer (obs/sub-observer observer name)
        db-conf (bdb-conf read-only deferred-write cache-mode)
	
	;;never open environemtns readonly, see: http://forums.oracle.com/forums/thread.jspa?threadID=2239407&tstart=0
	env (bdb-env (if read-only-env
		       (do (assert read-only)			 
			 (assoc args :read-only true))
		       (dissoc args :read-only)))
	db (.openDatabase env nil name db-conf)]
    (->
     (reify IReadBucket
	    (bucket-get [this k]
			(let [entry-key (DatabaseEntry. (serialize k))
			      entry-val (DatabaseEntry.)]
			  (when (= (.get db nil entry-key entry-val LockMode/DEFAULT)
				   OperationStatus/SUCCESS)
			    (deserialize (.getData entry-val)))))
	    (bucket-batch-get [this ks]
			      (default-bucket-batch-get this ks))
	    ;;This method does an optimized, internal traversal, does not impact the working set in the cache, but may not be accurate in the face of concurrent modifications in the database
	    ;;see:  http://www.oracle.com/technetwork/database/berkeleydb/je-faq-096044.html#31
	    (bucket-count [this]
			  (.count db))
	    (bucket-keys [this]
              (cursor-seq db deserialize observer 
                          :keys-only true))
	    (bucket-seq [this]
              (cursor-seq db deserialize observer))

	    (bucket-exists? [this k]
			    (let [entry-key (DatabaseEntry. (serialize k))
				  entry-val (DatabaseEntry.)]
			      (.setPartial entry-val (int 0) (int 0) true)
			      (= (.get db nil entry-key entry-val LockMode/DEFAULT)
				 OperationStatus/SUCCESS)))

	    IOptimizeBucket
	    (bucket-optimize [this] (optimize-db db))
	    
	    IMergeBucket
	    (bucket-merge [this k v]
			  (default-bucket-merge this merge k v))

	    
	    IWriteBucket
	    (bucket-put [this k v]
			(let [entry-key (DatabaseEntry. (serialize k))
			      entry-val (DatabaseEntry. (serialize v))]
			  (.put db nil entry-key entry-val)))
	    (bucket-delete [this k]
			   (.delete db nil (DatabaseEntry. (serialize k))))
	    (bucket-update [this k f]
			   (default-bucket-update this k f))
	    (bucket-sync [this]
			 (when (-> db .getConfig .getDeferredWrite)
			   (.sync db)))
	    (bucket-close [this]
			  (do 
			    (.close db)
			    (.close env))))
     (?> merge with-flush merge))))