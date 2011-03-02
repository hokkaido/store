(ns store.daemon
  (:require [store.api :as store] [clojure.contrib.logging :as log])
  (:use [plumbing.core :only [with-log]]
        [clojure.string :only [lower-case]]))

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

(defn bucket-server [buckets]
  (with-log :error
    (fn [[op bname & args]]
      (let [op-key (-> op lower-case keyword)
	    b (buckets (-> bname keyword))
	    bop (op-map op-key)]
	[(apply bop b args)]))))