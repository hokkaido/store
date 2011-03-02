(ns store.net
  (:use store.api
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [plumbing.serialize :only [write-msg read-msg]]
        [plumbing.server :only [client]]
        [plumbing.core :only [with-timeout]])
  (:import (java.net Socket InetAddress)))

(defn net-bucket
  "Provides bucket impl for a network interface to a store."
  [& {:keys [^String name
             ^String host
             port
             timeout]
      :or {timeout 10}}]
  ;; Client will later use a pool
  (let [client (with-timeout timeout
                 (partial client host port (comp first read-msg) write-msg))]
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
                    (client ["CLOSE" name])))))