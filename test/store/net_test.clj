(ns store.net-test
  (:use clojure.test
	[plumbing.serialize :only [start handler]]
	[clojure.contrib.server-socket :only [close-server]]
	store.net
	store.daemon)
  (:require [store.api :as store]))

(deftest  get-put-test
  (let [s (start
           (handler (bucket-server
		     {:b1 (store/hashmap-bucket)}))
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

;;(deftest ^{:system true} timeout)