(ns store.core-test
  (:require [clj-time.core :as time]
	    [store.core :as bucket])
  (:use clojure.test
        [plumbing.core :only [find-first map-from-keys]]))

(defn generic-bucket-test [b]
  (bucket/put b "k1" "v1")
  (is (= (bucket/get b "k1") "v1"))
  (is (= 1 (count (filter (partial = "k1") (bucket/keys b)))))
  (is (bucket/exists? b "k1"))
  (bucket/delete b "k1")
  (is (not (bucket/exists? b "k1")))
  (bucket/put b "k2" {:a 1})
  (is (= 1 (-> b (bucket/get "k2") :a)))
  (bucket/put b "k2" 2)
  (is (= 2 (bucket/get b "k2")))
  (is (= [["k2",2]] (bucket/seq b)))
  (is (nil? (bucket/get b "dne")))
  (bucket/update b "k2" inc)
  (is (= (bucket/get b "k2") 3))
  (let [batch {"k3" 3
	       "k4" 4
	       "k5" 5}]
    (bucket/batch-put b batch)
    (is (= batch (into {} (bucket/batch-get b [ "k3"
						"k4"
						"k5"])))))
  (bucket/clear b))

(deftest mem-bucket-test
  (generic-bucket-test (bucket/bucket {:type :mem})))

(deftest fs-bucket-test
  (generic-bucket-test
   (bucket/bucket {:type :fs
	    :name "foo"
	    :path (.getAbsolutePath (java.io.File. "store-core-test-dir"))}))
  (.deleteOnExit (java.io.File. "store-core-test-dir")))

(deftest flush-test
  (let [b1 (bucket/bucket {:type :mem})
        b2 (bucket/bucket {:type :mem :merge (fn [_ v1 v2] (merge v1 v2))})]
    (bucket/put b1 :foo {:bar "bar"})
    (bucket/put b1 :nutty {:mcsackhang "mcsackhang"})
    (bucket/merge-to! b1 b2)
    (is (= (into {} (bucket/seq b2))
	   (into {} (bucket/seq b1))))
    (bucket/merge-to! {:bar {:balls "deep"}} b2)
    (is (= {:bar {:balls "deep"}
	    :foo {:bar "bar"}
	    :nutty {:mcsackhang "mcsackhang"}}
	   (into {} (bucket/seq b2))))))

(deftest with-flush-test
  (let [merge-fn (fnil (fn [_ v1 v2] (merge v1 v2)) {})
	b1 (bucket/bucket {:type :mem :merge merge-fn})
	b2 (bucket/with-flush b1 merge-fn)]
    (bucket/merge b2 "k" {"v1" "v"})
    (is (nil? (bucket/get b1 "k")))
    (bucket/sync b2)
    (is (= (bucket/get b1 "k") {"v1" "v"}))
    (is (= {"v1" "v"} (bucket/delete b2 "k")))
    (bucket/sync b2)
    (is (nil? (bucket/get b2 "k")))))

(deftest with-cache-test
  (let [merge-fn (fnil (fn [_ v1 v2] (merge v1 v2)) {})
	b1 (bucket/bucket {:type :mem :merge merge-fn})
	b2 (bucket/with-cache b1 merge-fn)]
    (bucket/merge b2 "k" {"v1" "v"})
    (bucket/merge b1 "k" {"you got borked" "heavy hangers"})
    (is (= {"v1" "v"} (bucket/get b2 "k")))
    (is (nil? (bucket/get b1 "k")))
    (bucket/sync b2)
    (is (= {"v1" "v"} (bucket/get b1 "k")))
    (is (= {"v1" "v"} (bucket/delete b2 "k")))
    (is (nil? (bucket/get b2 "k")))
    (bucket/sync b2)
    (is (nil? (bucket/get b2 "k")))))

(deftest bucket-counting-merge-test
  (let [n 1000
	b (bucket/bucket {:type :mem :merge (fn [_ sum x] (+ (or sum 0) x))})
	latch (java.util.concurrent.CountDownLatch. n)
	pool (java.util.concurrent.Executors/newFixedThreadPool 10)]
    (dotimes [_ n]
      (.submit pool (cast Runnable (fn []
				     (bucket/merge b "k" 1)
				     (.countDown latch)))))
    (.await latch)
    (bucket/get b "k")))
