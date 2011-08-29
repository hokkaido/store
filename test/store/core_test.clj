(ns store.core-test
  (:require [clj-time.core :as time])
  (:use clojure.test
        store.api
	store.core
        [plumbing.core :only [find-first map-from-keys]]))

(defn generic-bucket-test [b]
  (bucket-put b "k1" "v1")
  (is (= (bucket-get b "k1") "v1"))
  (is (find-first (partial = "k1") (bucket-keys b)))
  (is (bucket-exists? b "k1"))
  (bucket-delete b "k1")
  (is (not (bucket-exists? b "k1")))
  (bucket-put b "k2" {:a 1})
  (is (= 1 (-> b (bucket-get "k2") :a)))
  (bucket-put b "k2" 2)
  (is (= 2 (bucket-get b "k2")))
  (is (= [["k2",2]] (bucket-seq b)))
  (is (nil? (bucket-get b "dne")))
  (bucket-update b "k2" inc)
  (is (= (bucket-get b "k2") 3)))

(defn generic-store-test [store]
  (let [s (store ["b1","b2","b3"])
	p (partial s :get)]
    (s :put "b1" "k" "v")
    (is (= (s :get "b1" "k") "v"))
    (is (= (p "b1" "k") "v"))
    (is (= ["k"] (s :keys "b1")))
    (s :delete "b1" "k")
    (is (empty? (s :keys "b1")))
    (s :put "b2" "k2" {:a 1})
    (is (= [["k2" {:a 1}]] (s :seq "b2")))
    (is (= [["k2" {:a 1}]] (bucket-seq (:read (s :bucket "b2")))))
    (is (= 1 (:a (s :get "b2" "k2"))))
    (s :update "b1" "k-count" (fnil inc 0))
    (is (= (s :get "b1" "k-count") 1))))

(deftest mem-bucket-test
  (generic-bucket-test (bucket {:type :mem})))

(deftest hashmap-store-test
  (generic-store-test (fn [names]
			(store names {:type :mem})))
  (let [s (store ["b1"] {:type :mem})]
    (s :put "b1" "k" "v")    
    (is (= ["k"] (s :keys "b1")))
    (is (= "v" (s :get "b1" "k")))))

(deftest fs-bucket-test
  (generic-bucket-test
   (bucket {:type :fs
	    :name "foo"
	    :path (.getAbsolutePath (java.io.File. "store-core-test-dir"))}))
  (.deleteOnExit (java.io.File. "store-core-test-dir")))


(deftest fs-store-test
  (let [root (java.io.File. ".")
        s (-> (store ["fs"] {:type :fs
			     :path (.getAbsolutePath root)}))]
    (s :put "fs" "my-key" 2)
    (is (.exists (java.io.File. root "fs/my-key"))))

  (let [root (java.io.File. ".")
        s (-> (store ["fs"] {:type :fs
			     :path root}))]
    (is (= (s :get "fs" "my-key") 2))
    (s :delete "fs" "my-key")
    (is (not (.exists (java.io.File. root "my-key"))))))

(deftest flush-test
  (let [b1 (bucket {:type :mem})
        b2 (bucket {:type :mem :merge (fn [_ v1 v2] (merge v1 v2))})]
    (bucket-put b1 :foo {:bar "bar"})
    (bucket-put b1 :nutty {:mcsackhang "mcsackhang"})
    (bucket-merge-to! b1 b2)
    (is (= (into {} (bucket-seq b2))
	   (into {} (bucket-seq b1))))
    (bucket-merge-to! {:bar {:balls "deep"}} b2)
    (is (= {:bar {:balls "deep"}
	    :foo {:bar "bar"}
	    :nutty {:mcsackhang "mcsackhang"}}
	   (into {} (bucket-seq b2))))))

(deftest with-flush-test
  (let [merge-fn (fnil (fn [_ v1 v2] (merge v1 v2)) {})
	b1 (bucket {:type :mem :merge merge-fn})
	b2 (with-flush b1 merge-fn)]
    (bucket-merge b2 "k" {"v1" "v"})
    (is (nil? (bucket-get b1 "k")))
    (bucket-sync b2)
    (is (= (bucket-get b1 "k") {"v1" "v"}))))



(deftest bucket-counting-merge-test
  (let [n 1000
	b (bucket {:type :mem :merge (fn [_ sum x] (+ (or sum 0) x))})
	latch (java.util.concurrent.CountDownLatch. n)
	pool (java.util.concurrent.Executors/newFixedThreadPool 10)]
    (dotimes [_ n]
      (.submit pool (cast Runnable (fn []
				     (bucket-merge b "k" 1)
				     (.countDown latch)))))
    (.await latch)
    (bucket-get b "k")))