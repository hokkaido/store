(ns store.daemon
  (:require [store.api :as store] [clojure.contrib.logging :as log])
  (:use [plumbing.core :only [with-log]]
        [clojure.contrib.server-socket :only [create-server
                                              close-server]]
        [clojure.string :only [lower-case]]
        [store.message :only [read-msg write-msg]])
  (:import (java.net InetAddress Socket)
           (org.apache.commons.io IOUtils)
           (java.io InputStream OutputStream)))

(defn start [fun
             & {:keys [port backlog bind-addr]
                :or {port 4444
                     backlog 50
                     bind-addr (InetAddress/getByName "127.0.0.1")}}]
  (let [server (create-server port fun backlog bind-addr)]
    server))

;; TODO: fix double serialization
(def op-map
  {:get store/bucket-get
   :exists store/bucket-exists?
   :keys store/bucket-keys
   :seq store/bucket-seq
   :modified store/bucket-modified
   :merge store/bucket-merge
   :put store/bucket-put
   :delete store/bucket-delete
   :sync store/bucket-sync
   :close store/bucket-close})

(defn handler [buckets]
  "Map of buckets."
  (let [exec-req (with-log :error
		   (fn [[op bname & args]]
		     (let [op-key (-> op lower-case keyword)
			   b (buckets (-> bname keyword))
			   bop (op-map op-key)]
		       [(apply bop b args)])))]
    (fn [^InputStream is ^OutputStream os]
      (let [i (read-msg is)]
	(write-msg os (exec-req i))
	(.flush os)))))