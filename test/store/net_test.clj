(ns store.net-test
  (:use clojure.test
        store.api
        store.net
        [store.core-test :only [generic-bucket-test]]
        [plumbing.core :only [find-first]]
        [plumbing.error :only [with-ex logger]])
  (:require [store.api :as store]))

(use-fixtures :each
              (fn [f]
                (let [server (start-rest-bucket-server
                              {"b1" (hashmap-bucket)
                               "b2" (hashmap-bucket)}
                              :port 4445
                              :join? false)]
                  (f)
                  (.stop server))))

(deftest exec-req-test
  (let [bs {"hm" (hashmap-bucket)
            "fs" (fs-bucket "/tmp/store-net-test")}]
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-req bs {:name "hm" :op "put"} "k1" "v1")))
    (is (= {:body "\"v1\""
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-req bs {:name "hm" :op "get"} "k1")))

    ;; NPE since hashmaps don't support nil vals
    (is (= {:body "{\"error\":\"java.lang.NullPointerException\"}"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 500}
           (exec-req bs {:name "hm" :op "put"} "null-k" nil)))
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-req bs {:name "fs" :op "put"} "null-k" nil)))
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-req bs {:name "fs" :op "get"} "null-k")))))

(deftest rest-bucket-test
  (let [b (rest-bucket :name "b1"
                       :host "localhost"
                       :port 4445)]
    (bucket-put b "k1" "v1")
    (is (= (bucket-get b "k1") "v1"))
    (is (find-first (partial = "k1") (bucket-keys b)))
    (is (bucket-exists? b "k1"))

    (bucket-delete b "k1")
    (is (not (bucket-exists? b "k1")))

    (bucket-put b "k2" {:a 1})
    (is (= 1
           (-> b (bucket-get "k2") (get "a"))))

    (bucket-put b "k2" 2)
    (is (= 2 (bucket-get b "k2")))
    (is (= [["k2",2]] (bucket-seq b)))
    (is (nil? (bucket-get b "dne")))

    (bucket-update b "k2" inc)
    (is (= (bucket-get b "k2") 3))))