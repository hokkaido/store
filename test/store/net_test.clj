(ns store.net-test
  (:use clojure.test
        store.api
	store.core
        store.net
        [store.core-test :only [generic-bucket-test]]
        [plumbing.core :only [find-first]]
        [plumbing.error :only [with-ex logger]]
	[ring.adapter.jetty :only [run-jetty]]
	[compojure.core :only [routes]])
  (:require [store.api :as store] [clj-json.core :as json]))

(use-fixtures :each
              (fn [f]
                (let [handlers (rest-store-handler
				(store ["b1" "b2"]))
		      server (run-jetty
			      (apply routes handlers)
			      {:port 4445
			       :join? false})]
                  (f)
                  (.stop server))))

(deftest exec-req-test
  (let [s (store [{:name "hm"
		   :merge (fn [_ x y] (+ (or x 0) y))}
		  {:name "fs"
		   :type :fs
		   :path "/tmp/store-net-test"}])]
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-request s {:name "hm" :op "put"} "k1" 42)))
    (is (= {:body "42"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-request s {:name "hm" :op "get"} "k1")))

    (exec-request s {:name "hm" :op "put"} "k2" 42)
    (is (= #{"k1" "k2"}
	   (into #{} (map json/parse-string (:body (exec-request s {:name "hm" :op "keys"}))))))

    ;; NPE since hashmaps don't support nil vals
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-request s {:name "fs" :op "put"} "null-k" nil)))
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (exec-request s {:name "fs" :op "get"} "null-k")))

    ;; Merge
    (exec-request s {:name "hm" :op "merge"} "k1" 42)
    (is (= "84"
	   (:body (exec-request s {:name "hm" :op "get"}  "k1"))))))

(deftest rest-bucket-test
  (let [b (rest-bucket :name "b1"
                       :host "localhost"
		       :batch-size 2
                       :port 4445)]
    (bucket-put b "k1" "v1")
    (is (= (bucket-get b "k1") "v1"))
    (is (find-first (partial = "k1") (bucket-keys b)))
    (is (bucket-exists? b "k1"))

    (is (= [["k1" "v1"]] (bucket-seq b)))
    
    (bucket-put b "k2" {:a 1})
    (is (= 1
           (-> b (bucket-get "k2") :a)))

    (is (= #{"k1" "k2"} (into #{} (bucket-keys b))))
    
    (bucket-delete b "k1")
    (is (not (bucket-exists? b "k1")))

    
    (bucket-put b "k2" 2)
    (is (= 2 (bucket-get b "k2")))
    (is (= [["k2",2]] (bucket-seq b)))
    (is (nil? (bucket-get b "dne")))

    (bucket-update b "k2" inc)
    (is (= (bucket-get b "k2") 3))

    (bucket-put b "k3" {:a 1})
    (is (= {:a 1} (bucket-get b "k3")))

    (bucket-put b "k1" "v1")
    (bucket-put b "k2" "v2")
    (bucket-put b "k3" "v3")
    (bucket-put b "k4" "v4")
    (is (=
	 (seq {"k1" "v1"
	   "k2" "v2"
	   "k3" "v3"
	   "k4" "v4"})
	  (sort-by first (bucket-batch-get b ["k1" "k2" "k3" "k4"]))))))