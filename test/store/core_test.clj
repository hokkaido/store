(ns store.core-test
  (:use clojure.test
        store.api
	store.riak
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
    (is (= 1 (:a (s :get "b2" "k2"))))
    (s :update "b1" "k-count" (fnil inc 0))
    (is (= (s :get "b1" "k-count") 1))))

(deftest hashmap-bucket-test
  (generic-bucket-test (hashmap-bucket)))

(deftest hashmap-store-test
  (generic-store-test (fn [names]
			(mk-store (map-from-keys (fn [& _] (hashmap-bucket)) names))))
  (let [s (mk-store {"b1" (hashmap-bucket)})]
    (s :put "b1" "k" "v")    
    (is (= ["k"] (s :keys "b1")))
    (is (= "v" (s :get "b1" "k")))))

(deftest asymetric-read-write
  (let [reader (hashmap-bucket)
	writer (hashmap-bucket)
	store (mk-store {"a" reader} {"a" writer})]
    (store :put "a" "k" "v")
    (is (= nil (store :get "a" "k")))
    (is (= "v" (bucket-get writer "k")))
    (bucket-put reader "k1" "v1")
    (is (= "v1" (store :get "a" "k1")))))

(deftest fs-bucket-test
  (generic-bucket-test
   (fs-bucket
    (.getAbsolutePath (java.io.File. "store-core-test-dir"))))
  (.deleteOnExit (java.io.File. "store-core-test-dir")))

(deftest fs-store-test
  (let [root (java.io.File. ".")
        s (-> (mk-store {"fs" (fs-bucket (.getAbsolutePath root))}))]
    (s :put "fs" "my-key" 2)
    (is (.exists (java.io.File. root "my-key"))))

  (let [root (java.io.File. ".")
        s (-> (mk-store {"fs" (fs-bucket root)}))]
    (is (= (s :get "fs" "my-key") 2))
    (s :delete "fs" "my-key")
    (is (not (.exists (java.io.File. root "my-key"))))))

;; (deftest ^{:system true :redis true}
;;   redis-bucket-test
;;   (generic-bucket-test (redis-bucket "b" default-redis-config)))

;; (deftest ^{:system true :redis true}
;;   redis-store-test
;;   (generic-store-test (fn [names]
;; 			(map-from-keys redis-bucket names)))
;;   (let [s (mk-store
;; 	   (map-from-keys redis-bucket ["b1" "b2"]))]
;;     (s :put "b1" "k" "v")
;;     (is (= "v" (s :get "b1" "


(deftest flush-test
  (let [b1 (hashmap-bucket)
        b2 (hashmap-bucket)]
    (bucket-put b1 :foo {:bar "bar"})
    (bucket-put b1 :nutty {:mcsackhang "mcsackhang"})
    (bucket-merge-to! b1 (with-merge b2 (fn [_ v1 v2] (merge v1 v2))))
    (is (= (into {} (bucket-seq b2))
	   (into {} (bucket-seq b1))))
    (bucket-merge-to! {:bar {:balls "deep"}}
		      (with-merge b2 (fn [k v1 v2]
					(merge v1 v2))))
    (is (= {:bar {:balls "deep"}
	    :foo {:bar "bar"}
	    :nutty {:mcsackhang "mcsackhang"}}
	   (into {} (bucket-seq b2))))))

(deftest with-flush-test
  (let [b1 (with-merge (hashmap-bucket) (fnil (fn [_ v1 v2] (merge v1 v2)) {}))
	b2 (with-flush b1)]
    (bucket-merge b2 "k" {"v1" "v"})
    (is (nil? (bucket-get b1 "k")))
    (bucket-sync b2)
    (is (= (bucket-get b1 "k") {"v1" "v"}))))

(deftest with-flush-multicast-test
  (let [shitty-bucket #(with-merge
                         (hashmap-bucket)
                         (fnil (fn [_ v1 v2] (merge v1 v2)) {}))
        remote-buckets
        [(shitty-bucket)
         (shitty-bucket)]
        local-bucket (shitty-bucket)
        b2 (with-flush (cons local-bucket remote-buckets))]
    (bucket-merge b2 "k" {"v1" "v"})
    (doseq [x (cons local-bucket remote-buckets)]
      (is (nil? (bucket-get x "k"))))
    (bucket-sync b2)
    (doseq [x (concat [local-bucket] remote-buckets)]
      (is (= (bucket-get x "k")  {"v1" "v"})))))

(deftest with-multicast-test
  (let [b1 (hashmap-bucket)
        b2 (hashmap-bucket)
        mb (with-multicast [b1 b2])]
    (bucket-put mb "k1" "v1")
    (bucket-put mb "k2" "v2")
    (are [x y] (= x y)
         "v1" (bucket-get b1 "k1")
         "v2" (bucket-get b1 "k2")
         "v1" (bucket-get b2 "k1")
         "v2" (bucket-get b2 "k2"))))

(deftest add-listeners-test
  (let [b  (hashmap-bucket)
	b1 (hashmap-bucket)
        b2 (hashmap-bucket)
        mb (add-write-listeners b [b1 b2])]
    (bucket-put mb "k1" "v1")
    (is (= (bucket-seq mb)  [["k1" "v1"]]))
    (is (= (bucket-get mb "k1") "v1"))
    (is (= (bucket-get b1 "k1") "v1"))
    (is (= (bucket-get b2 "k1") "v1"))))

(deftest with-layers-test
  (let [top (hashmap-bucket)
        mid (hashmap-bucket)
        bot (hashmap-bucket)
        lb (with-layers [top mid bot])]
    (bucket-put top "k1" "vt")
    (bucket-put mid "k1" "vm")
    (bucket-put bot "k1" "vb")
    (is (= "vt" (bucket-get lb "k1")))

    (bucket-put mid "k2" "vvm")
    (bucket-put bot "k2" "vvb")
    (is (= "vvm" (bucket-get lb "k2")))

    (bucket-put bot "k3" "vvvb")
    (is (= "vvvb" (bucket-get lb "k3")))

    (is (= nil (bucket-get lb "dne")))

    (is (bucket-exists? lb "k1"))
    (is (bucket-exists? lb "k2"))
    (is (bucket-exists? lb "k3"))
    (is (not (bucket-exists? lb "dne")))))