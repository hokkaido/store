(ns store.net
  (:use store.api
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.serialize :only [reader writer read-str-msg write-str-msg]]
        [plumbing.server :only [client]]
        [plumbing.core :only [with-timeout with-log]]
	[clojure.string :only [lower-case]])
  (:import (java.net Socket InetAddress)))

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

(defn bucket-server [buckets]
  (with-log :error
    (fn [[op bname & args]]
      (let [op-key (-> op lower-case keyword)
	    b (buckets (-> bname keyword))
	    bop (op-map op-key)]
	[(apply bop b args)]))))

(defn net-bucket
  "Provides bucket impl for a network interface to a store."
  [& {:keys [^String name
             ^String host
             port
             timeout]
      :or {timeout 10}}]
  ;; Client will later use a pool
  (let [client (with-timeout timeout
                 (partial client host port
			  (comp first read-str-msg reader)
			  (fn [s msg]
			    (write-str-msg (writer s) msg))))]
    (reify
      IReadBucket
      (bucket-get [this k]
                  (client ["GET" name k]))
      (bucket-keys [this]
                   (client ["KEYS" name]))
      (bucket-seq [this]
                  (client ["SEQ" name]))
      (bucket-exists? [this k]
                      (client ["EXISTS" name k]))

      IWriteBucket
      (bucket-put [this k v]
                  (client ["PUT" name k v]))
      (bucket-delete [this k]
                     (client ["DELETE" name k]))
      (bucket-update [this k f]
                     (client ["UPDATE" name k]))
      (bucket-sync [this]
                   (client ["SYNC" name]))
      (bucket-close [this]
                    (client ["CLOSE" name]))
      (bucket-merge [this k v]
		    (client ["MERGE" name k v])))))