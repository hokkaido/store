(ns store.net
  (:use store.api
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.serialize :only [reader writer read-str-msg write-str-msg]]
        [plumbing.server :only [client server start]]
        [plumbing.core :only [with-timeout with-log]]
	[clojure.string :only [lower-case]])
  (:import
   (java.net Socket InetAddress)
   (java.util.concurrent Executors Future Callable TimeUnit)))

(def op-map
  {:get bucket-get
   :exists bucket-exists?
   :keys bucket-keys
   :seq bucket-seq
   :modified bucket-modified
   :merge bucket-merge
   :put bucket-put
   :delete bucket-delete
   :sync bucket-sync
   :close bucket-close})

(defn- bucket-server [buckets]
  (with-log :error
    (fn [[op bname & args]]
      (let [op-key (-> op lower-case keyword)
	    b (buckets (-> bname keyword))
	    bop (op-map op-key)]
	[(apply bop b args)]))))

(defn start-net-bucket-server [buckets port]
  (start
   (with-log :error
     (partial server (bucket-server
		      {:b1 (hashmap-bucket)
		      :b2 (hashmap-bucket)})
	     (comp read-str-msg reader)
	     (fn [s msg]
	       (write-str-msg (writer s) msg))))))

(defn net-bucket-client [host port]
     (partial client host port
	      (comp first read-str-msg reader)
	      (fn [s msg]
		(write-str-msg (writer s) msg))))

(defn mk-client-exec [c num-threads future-policy]
  (let [pool (Executors/newSingleThreadExecutor)]
    [(with-log :error
	(fn [args]
	  (let [^Callable work #(c args)
		f (.submit pool work)]
	    (future-policy f))))
     pool]))

(defn net-bucket
  "Provides bucket impl for a network interface to a store."
  [& {:keys [^String name
             ^String host
             port
             timeout
	     wait-on-write
	     writer-threads
	     reader-threads]
      :or {timeout 10
	   writer-threads 10
	   reader-threads 10
	   wait-on-write true}}]
  ;; Client will later use a pool
  (let [c (net-bucket-client host port)
	[reader-exec _] (mk-client-exec c reader-threads
				 (fn [^Future f]
				   (.get f (int timeout) (TimeUnit/SECONDS))))
	[writer-exec _] (mk-client-exec c writer-threads
				 (fn [^Future f]
				   (when wait-on-write
				     (.get f (int timeout) (TimeUnit/SECONDS)))))]
    (reify
      IReadBucket
      (bucket-get [this k] (reader-exec ["GET" name k]))                   
      (bucket-keys [this] (reader-exec ["KEYS" name]))                   
      (bucket-seq [this] (reader-exec ["SEQ" name]))                  
      (bucket-exists? [this k] (reader-exec ["EXISTS" name k]))
                       
      IWriteBucket
      (bucket-put [this k v] (writer-exec ["PUT" name k v]))
      (bucket-delete [this k] (writer-exec ["DELETE" name k]))		     
      (bucket-update [this k f] (writer-exec ["UPDATE" name k]))		     
      (bucket-sync [this] (writer-exec ["SYNC" name]))		   
      (bucket-close [this] (writer-exec ["CLOSE" name]))	   
      (bucket-merge [this k v] (writer-exec ["MERGE" name k v])))))		   