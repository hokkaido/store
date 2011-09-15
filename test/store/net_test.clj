(ns store.net-test
  (:use clojure.test
        store.net
        [store.core-test :only [generic-bucket-test]]
        [plumbing.core :only [find-first]]
        [plumbing.error :only [with-ex logger]]
	[ring.adapter.jetty :only [run-jetty]]
	[compojure.core :only [routes]])
  (:require [store.api :as store]
	    [store.core :as bucket]
	    [clj-json.core :as json]))

(use-fixtures :each
              (fn [f]
                (let [server (store-server
			      (store/store ["b1" "b2"]
				     {:merge (fn [_ x y]
					       (+ (or x 0) y))}))]
                  (f)
                  (.stop server))))

(def rest-spec
     {:host "localhost"
      :port 4445
      :type :rest})

(deftest fetch-test
  (is (= "text/javascript; charset=UTF-8"
       (-> (fetcher.core/fetch :get "http://localhost:4445/store/get/b1/k?callback=x")
	   :headers
	   (get "content-type")))))

(deftest exec-req-test
  (let [s (store/store [{:name "hm"
		   :merge (fn [_ x y] (+ (or x 0) y))}
		  {:name "fs"
		   :type :fs
		   :path "/tmp/store-net-test"}])]
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (handle-request s {:name "hm" :op "put"} "k1" 42)))
    (is (= {:body "42"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (handle-request s {:name "hm" :op "get"} "k1")))

    (is (= {:body "callback(42)"
	    :headers {"Content-Type" "text/javascript; charset=UTF-8"}
	    :status 200}
	   (handle-request s {:name "hm" :op "get" "callback" "callback"} "k1")))


    (is (= {:body "callback([[\"k1\",42]])"
	    :headers {"Content-Type" "text/javascript; charset=UTF-8"}
	    :status 200}
	   (handle-request s {:name "hm" :op "seq" "callback" "callback"})))

    
    (handle-request s {:name "hm" :op "put"} "k2" 42)
    (is (= #{"k1" "k2"}
	   (into #{} (map json/parse-string (:body (handle-request s {:name "hm" :op "keys"}))))))

    ;; NPE since hashmaps don't support nil vals
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (handle-request s {:name "fs" :op "put"} "null-k" nil)))
    (is (= {:body "null"
            :headers {"Content-Type" "application/json; charset=UTF-8"}
            :status 200}
           (handle-request s {:name "fs" :op "get"} "null-k")))

    ;; Merge
    (handle-request s {:name "hm" :op "merge"} "k1" 42)
    (is (= "84"
	   (:body (handle-request s {:name "hm" :op "get"}  "k1"))))))

(deftest rest-bucket-test
  (let [b (bucket/bucket {:type :rest
		   :name "b1"
		   :host "localhost"
		   :batch-size 2
		   :port 4445})]
    (bucket/put b "k1" "v1")
    (is (= (bucket/get b "k1") "v1"))
    (is (find-first (partial = "k1") (bucket/keys b)))
    (is (bucket/exists? b "k1"))

    (is (= [["k1" "v1"]] (bucket/seq b)))
    
    (bucket/put b "k2" {:a 1})
    (is (= 1
           (-> b (bucket/get "k2") :a)))

    (is (= #{"k1" "k2"} (into #{} (bucket/keys b))))

    (let [batch {"k3" 3
		 "k4" 4
		 "k5" 5}]

      (bucket/batch-put b batch)
      (is (= batch (into {}  (bucket/batch-get b [ "k3"
						   "k4"
						   "k5"])))))
    
    (bucket/clear b)
    (is (not (bucket/exists? b "k1")))

    
    (bucket/put b "k2" 2)
    (is (= 2 (bucket/get b "k2")))
    (is (= [["k2",2]] (bucket/seq b)))
    (is (nil? (bucket/get b "dne")))

    (bucket/merge b "k2" 1)
    (is (= (bucket/get b "k2") 3))

    (bucket/put b "k3" {:a 1})
    (is (= {:a 1} (bucket/get b "k3")))

    (bucket/put b "k1" "v1")
    (bucket/put b "k2" "v2")
    (bucket/put b "k3" "v3")
    (bucket/put b "k4" "v4")
    (is (=
	 (seq {"k1" "v1"
	   "k2" "v2"
	   "k3" "v3"
	   "k4" "v4"})
	 (sort-by first (bucket/batch-get b ["k1" "k2" "k3" "k4"]))))))

(deftest rest-store-test
  (let [s (store/store [] rest-spec)]
    (s :add "b3")
    (s :put "b3" "k1" 1)
    (is (= 1 (s :get "b3" "k1")))
    (is (s :bucket "b3"))
    (s :remove "b1")
    (s :remove "b2")
    (let [other (store/store [] rest-spec)]
      (is (= ["b3"] (other :buckets)))

      (s :remove "b3")
      (is (not (s :bucket "b3")))
      (is (empty? (other :buckets))))))

(deftest mirror-remote-store-test
  (let [s1 (store/mirror-remote rest-spec)
	s2 (store/mirror-remote rest-spec)]
    (is (= ["b1" "b2"] (sort  (s1 :buckets))))
      (s2 :remove "b2")
      (is (not (s1 :bucket "b2")))
      (is (= ["b1"] (s1 :buckets)))))

(deftest rest-client-keywords-test
  (let [b (bucket/bucket {:type :rest
		   :name "b1"
		   :host "localhost"
		   :batch-size 2
		   :port 4445})
	body (fetcher.core/fetch :get "http://www.google.com")
	pool (java.util.concurrent.Executors/newFixedThreadPool 100)
	tasks (range 10)
	latch (java.util.concurrent.CountDownLatch. (count tasks))]
    (doseq [x tasks]
      (.submit pool
	       (cast Runnable
		     #(do (bucket/put b
				      (str "http://www.google.com" x)
				      body)
			  (.countDown latch)))))
    (.await latch)
    (.shutdown pool)
    (let [ks (keys (bucket/get b "http://www.google.com0"))]
      (is (empty? (remove keyword? ks))))
    (is (= 10 (count (bucket/keys b))))))

(deftest rest-client-strings-test
  (let [b (bucket/bucket {:type :rest
		   :name "b1"
		   :host "localhost"
		   :batch-size 2
		   :port 4445
		   :keywordize? false})
	body (fetcher.core/fetch :get "http://www.google.com")
	pool (java.util.concurrent.Executors/newFixedThreadPool 100)
	tasks (range 10)
	latch (java.util.concurrent.CountDownLatch. (count tasks))]
    (doseq [x tasks]
      (.submit pool
	       (cast Runnable
		     #(do (bucket/put b
				      (str "http://www.google.com" x)
				      body)
			  (.countDown latch)))))
    (.await latch)
    (.shutdown pool)
    (let [ks (keys (bucket/get b "http://www.google.com0"))]
      (is (empty? (remove string? ks))))
    (is (= 10 (count (bucket/keys b))))))