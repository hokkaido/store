(ns store.net-test
  (:use clojure.test
        [plumbing.server :only [start server]]
        [plumbing.serialize :only [read-msg write-msg]]
        [clojure.contrib.server-socket :only [close-server]]
        store.net
        store.daemon)
  (:require [store.api :as store]
            [store.bdb :as bdb]))

(deftest get-put-test
  (let [s (start
           (partial server (bucket-server
                            {:b1 (store/hashmap-bucket)})
                    read-msg write-msg)
           :port 4444)
        b (net-bucket :name "b1"
                      :host "127.0.0.1"
                      :port 4444)]
    (is (nil? (store/bucket-get b "k1")))
    (store/bucket-put b "k1" "v1")
    (is (= "v1"
           (store/bucket-get b "k1")))
    (store/bucket-put b "k2" 5)
    (is (= 5
           (store/bucket-get b "k2")))
    (store/bucket-put b
                      "k3"
                      {:a1 {:b11 "b11val" :b12 12}
                       :a2 {:b21 "b21val" :b22 22}})
    (is (= {:a1 {:b11 "b11val" :b12 12}
            :a2 {:b21 "b21val" :b22 22}}
           (store/bucket-get b "k3")))

    (close-server s)))

(deftest bdb-server-test
  (let [db-env (bdb/bdb-env :path "/tmp/nettest"
                            :cache-percent 35
                            :clean-util-thresh 75
                            :checkpoint-high-priority? true
                            :num-cleaner-threads 3
                            :locking true)
        s (start
           (partial server (bucket-server
                            {:b1 (bdb/bdb-bucket
                                  (bdb/bdb-db "b1" db-env
                                              :cache-mode :evict-ln))})
                    read-msg write-msg)
           :port 4445)
        b (net-bucket :name "b1"
                      :host "127.0.0.1"
                      :port 4445)]
    (is (nil?
         (store/bucket-get b "k1")))

    (store/bucket-put b "k1" {:foo 0.99})

    (is (= {:foo 0.99}
           (store/bucket-get b "k1")))

    (close-server s)))
