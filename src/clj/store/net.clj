(ns store.net
  (:use store.api
        [clojure.java.io :only [file copy]]
        [clojure.contrib.shell :only [sh]]
        [store.message :only [write-msg read-msg]])
  (:import (java.net Socket InetAddress)))

(defn client-socket [^String host ^Integer port f]
  (let [client (Socket. (InetAddress/getByName host) port)
        os (.getOutputStream client)
        ins (.getInputStream client)]
    (f ins os)))

(defn req [cmd]
  (fn [^InputStream ins
       ^OutputStream os]
    (write-msg os cmd)
    (-> (read-msg ins)
        first)))

(defn net-bucket
  "Provides bucket impl for a network interface to a store."
  [& {:keys [^String name
             ^String host
             port]}]
  ;; Client will later use a pool
  (let [client (partial client-socket host port)]
    (reify
      IReadBucket
      (bucket-get [this k]
                  (client (req ["GET" name k])))
      (bucket-keys [this]
                   (client (req ["KEYS" name])))
      (bucket-seq [this]
                  (client (req ["SEQ" name])))
      (bucket-exists? [this k]
                      (client (req ["EXISTS" name k])))

      IWriteBucket
      (bucket-put [this k v]
                  (client (req ["PUT" name k v])))
      (bucket-delete [this k]
                     (client (req ["DELETE" name k])))
      (bucket-update [this k f]
                     (client (req ["UPDATE" name k])))
      (bucket-sync [this]
                   (client (req ["SYNC" name])))
      (bucket-close [this]
                    (client (req ["CLOSE" name]))))))