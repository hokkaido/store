(ns store.api-test
  (:use clojure.test)
  (:require [store.core :as bucket]
	    [store.api :as store]))

(deftest build-memory-buckets
  (let [s (store/store [{:name "foo"}]
		    {:type :mem})]
    (s :put "foo" :a 1)
    (= 1 (s :get "foo" :a))))

(deftest bucket-test
  (let [s (store/store [{:name "foo"
		    :type :mem
		    :merge (fn [_ x y] (+ (or x 0) (or y 0)))}])]
    (s :merge "foo" :a 2)
    (is (= 2 (s :get "foo" :a)))))

(deftest build-merge-buckets
  (let [s (store/store [{:name "foo"
		   :merge (fn [_ x y] (+ (or x 0) (or y 0)))}]
		 {:type :mem})]
    (s :merge "foo" :a 1)
    (s :merge "foo" :a 4)
    (= 5 (s :get "foo" :a))))

(deftest store-op-test
  (let [s (store/store [])]
    (s :add "b")
    (s :put "b" :k 42)
    (is (= 42
	   (s :get "b" :k)))))

(deftest start-flush-pools-test
  (let [s (store/store ["b"] {:merge (fn [_ sum x] (+ (or sum 0) x))
			:flush-freq 1})
	b (bucket/get (.bucket-map s) "b")]
    (s :merge "b" "k" 42)
    (Thread/sleep 2000)
    (is (= 42 (s :get "b" "k")))
    (store/shutdown s)
    (Thread/sleep 1000)
    (is (.isShutdown (:flush-pool b)))))

(deftest store-with-dynamic-bucket
  (let [s (store/store [{:name "foo"
		   :merge (fn [_ x y]
			    (+ (or x 0) (or y 0)))}]
		 {:type :mem})]
    (s :put "foo" :a 1)
    (s :add "bar")
    (s :put "bar" :b 2)

    (= 1 (s :get "foo" :a))
    (= 2 (s :get "bar" :b))))

(deftest store-clone-test
  (let [s (store/store ["foo" "bar"])]
    (s :put "foo" :a 1)
    (is (= ["foo" "bar"] (s :buckets)))
    (= 1 (s :get "foo" :a))
    (let [c (store/clone s)]
      (c :put "bar" :b 2)
      (= 2 (c :get "bar" :b))
      (= 1 (s :get "foo" :a)))))

(deftest merge-stores-test
  (let [a (store/store ["a"])
	b (store/store ["b"])
	ab (store/merge-stores a b)]
    (ab :put "a" "1" "1")
    (ab :put "b" "2" "2")
    (is (= "1" (ab :get "a" "1")))
    (is (= "2" (ab :get "b" "2")))))

(deftest copy-test
  (let [a (store/store ["a"])
	b (store/store ["a"])
	kvs (map (fn [k]
		   [k {:hang k :bang k}])
		 (range 10000))]
    (doseq [[k v] kvs]
      (a :put "a" k v))
    (store/copy a b "a" :select :hang :threads 10)
    (is (= 10000 (count (a :keys "a"))))
    (is (= (range 10000)
	   (sort (map second (b :seq "a")))))))

(deftest sync-test
  (let [a (store/store ["a"])
	b (store/store ["a"])
	kvs (map (fn [k]
		   [k {:hang k :bang k}])
		 [1 2 3 4 5])]
    (doseq [[k v] kvs]
      (a :put "a" k v))
    (b :put "a" 3 100)
    (store/sync-stores a b "a" :select :hang :threads 10)
    (is (= 5 (count (a :keys "a"))))
    (is (= [1 2 4 5 100]
	     (sort (map second (b :seq "a")))))))

(deftest eval-test
  (let [s (store/store ["a"])]
    (s :exec
       (fn [x] (x :put "a" "b" "c")))
    (is (= "c" (s :get "a" "b")))))

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
    (is (= [["k2" {:a 1}]] (bucket/seq (:read (s :bucket "b2")))))
    (is (= 1 (:a (s :get "b2" "k2"))))
    (s :update "b1" "k-count" (fnil inc 0))
    (is (= (s :get "b1" "k-count") 1))))

(deftest hashmap-store-test
  (generic-store-test (fn [names]
			(store/store names {:type :mem})))
  (let [s (store/store ["b1"] {:type :mem})]
    (s :put "b1" "k" "v")    
    (is (= ["k"] (s :keys "b1")))
    (is (= "v" (s :get "b1" "k")))))

(deftest fs-store-test
  (let [root (java.io.File. ".")
        s (-> (store/store ["fs"] {:type :fs
			     :path (.getAbsolutePath root)}))]
    (s :put "fs" "my-key" 2)
    (is (.exists (java.io.File. root "fs/my-key"))))

  (let [root (java.io.File. ".")
        s (-> (store/store ["fs"] {:type :fs
			     :path root}))]
    (is (= (s :get "fs" "my-key") 2))
    (s :delete "fs" "my-key")
    (is (not (.exists (java.io.File. root "my-key"))))))