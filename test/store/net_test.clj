(ns store.net-test
  (:use clojure.test
	[store.daemon :only [start handler]]
	[clojure.contrib.server-socket :only [close-server]]
	store.net)
  (:require [store.api :as store]))

(deftest ^{:system true} get-put-test
  (let [s (start
           (handler {:b1 (store/hashmap-bucket)})
           :port 4444)
        b (net-bucket :name "b1"
                      :host "127.0.0.1"
                      :port 4444)]
    (is (= ""
           (store/bucket-get b "k1")))
    (is (= ""
           (store/bucket-put b "k1" "v1")))
    (is (= "v1"
           (store/bucket-get b "k1")))

    (close-server s)))