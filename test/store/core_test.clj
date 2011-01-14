(ns store.core-test
  (:use clojure.test
        store.api
        [plumbing.core :only [find-first]]))

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
  (is (nil? (bucket-get b "dne"))))

(defn generic-store-test [mk-store]
  (let [s (mk-store ["b1","b2","b3"])
	f (partial s :get)]
    (s :put "b1" "k" "v")
    (is (= (f "b1" "k") "v"))
    (is (= ["k"] (s :keys "b1")))
    (s :delete "b1" "k")
    (is (empty? (s :keys "b1")))
    (s :put "b2" "k2" {:a 1})
    (is (= [["k2" {:a 1}]] (s :seq "b2")))
    (is (= [["k2" {:a 1}]] (bucket-seq (s :bucket "b2"))))
    (is (= 1 (:a (s :get "b2" "k2"))))))

(deftest hashmap-bucket-test
  (generic-bucket-test (hashmap-bucket)))

(deftest hashmap-store-test
  (generic-store-test mk-hashmap-store)
  (let [s (mk-store {"b1" (hashmap-bucket)})]
    (s :put "b1" "k" "v")    
    (is (= ["k"] (s :keys "b1")))
    (is (= "v" (s :get "b1" "k")))))

(deftest fs-bucket-test
  (generic-bucket-test (fs-bucket (.getAbsolutePath (java.io.File. "store-core-test-dir"))))
  (.deleteOnExit (java.io.File. "store-core-test-dir")))

(deftest fs-store-test
  (let [root (java.io.File. ".")
	s (-> (mk-store {"fs" (fs-bucket (.getAbsolutePath root))}) )]
    (s :put "fs" "my-key" 2)
    (is (.exists (java.io.File. root "my-key")))
    (is (= (s :get "fs" "my-key") 2))
    (s :delete "fs" "my-key")
    (is (not (.exists (java.io.File. root "my-key"))))))

(deftest ^{:system true :redis true}
  redis-bucket-test
  (generic-bucket-test (redis-bucket "b" default-redis-config)))

(deftest ^{:system true :redis true}
  redis-store-test
  (generic-store-test mk-redis-store)
  (let [s (mk-redis-store ["b1" "b2"])]
    (s :put "b1" "k" "v")
    (is (= "v" (s :get "b1" "k")))))

(deftest ^{:system true :riak true}
  riak-bucket-test
  (let [b (riak-bucket :name "b")]
    (bucket-put b "k1" "v1")
    (is (= (bucket-get b "k1") "v1"))
    (is (find-first (partial = "k1") (bucket-keys b)))
    (is (bucket-exists? b "k1"))
    (bucket-delete b "k1")
    (is (not (bucket-exists? b "k1")))
    (bucket-put b "k2" {:a 1})
    (is (= 1 (-> b (bucket-get "k2") :a)))
    (Thread/sleep 1000)
    (bucket-put b "k2" 2)
    (is (= 2 (bucket-get b "k2")))
    (is (= [["k2",2]] (bucket-seq b)))
    (is (nil? (bucket-get b "dne")))))

(deftest ^{:system true :riak true}
  riak-store-test
  (let [s (mk-riak-store ["b1","b2","b3"])
	f (partial s :get)]
    (s :put "b1" "k" "v")
    (is (= (f "b1" "k") "v"))
    (is (= ["k"] (s :keys "b1")))
    (s :delete "b1" "k")
    (is (empty? (s :keys "b1")))
    (s :put "b2" "k2" {:a 1})
    (is (= [["k2" {:a 1}]] (s :seq "b2")))
    (is (= [["k2" {:a 1}]] (bucket-seq (s :bucket "b2"))))
    (is (= 1 (:a (s :get "b2" "k2"))))))