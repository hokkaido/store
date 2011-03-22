(ns store.bdb-test
  (:use clojure.test
	store.api
	store.bdb
	[store.core-test :only [generic-bucket-test]]
	[clojure.contrib.io :only [delete-file make-parents delete-file-recursively]]
	[plumbing.core :only [find-first]])
  (:import (com.sleepycat.je PreloadConfig)))

(defn- ensure-test-directory []
  (delete-file-recursively (java.io.File. "/tmp/bdbtest") true)
  (make-parents (java.io.File. "/tmp/bdbtest/ping"))
  (delete-file (java.io.File.  "/tmp/bdbtest/ping") true))

(deftest bdb-basics
  (ensure-test-directory)
  (let [db (bdb-db "bdb_test" (bdb-env :path "/tmp/bdbtest/"))
	_ (bdb-put db :foo [1 2 3])]
    (is (= [1 2 3] (bdb-get db :foo)))
    (is (= [:foo [1 2 3]] (first (entries-seq db))))
    (do (bdb-delete db :foo))
    (is (empty? (entries-seq db)))))

(deftest bdb-read-only-test
  (ensure-test-directory)
  (let [db (bdb-db "bdb_test" (bdb-env :path "/tmp/bdbtest/"))]
    (bdb-put db "k" "v"))
  (let [db-read (bdb-db "bdb_test" (bdb-env :path "/tmp/bdbtest/")
                        :read-only true)]
    (is (= "v" (bdb-get db-read "k")))))

(deftest bdb-deferred-write-test
  (ensure-test-directory)
  (let [db (bdb-db "bdb_test_deferred" (bdb-env :path "/tmp/bdbtest/")
                   :deferred-write true)]
    (bdb-put db "k" "v")
    (.close db))
  (let [db-read (bdb-db "bdb_test_deferred" (bdb-env :path "/tmp/bdbtest/")
                   :deferred-write false)]
    (is (= "v" (bdb-get db-read "k")))))

(deftest bdb-bucket-test
  (ensure-test-directory)
  (let [b (bdb-bucket (bdb-db "bdb_test" (bdb-env :path "/tmp/bdbtest")))]
    (generic-bucket-test b)))

(deftest bucket-keys-test
  (ensure-test-directory)
  (let [b (bdb-bucket
           (bdb-db "keys_test"
                   (bdb-env :path "/tmp/bdbtest")))]
    (bucket-put b "k1" "v1")
    (bucket-put b "k2" "v2")
    (bucket-put b "k3" "v3")
    (is (= '("k1" "k2" "k3")
           (sort (bucket-keys b))))))

(defn mk-data [n m]
  (reduce
   (fn [res i]
     (assoc res (str "key" i) (pr-str (range m))))
   {}
   (range n)))

;; (defn load-bucket [b kvs threads]
;;   (work/do-work
;;    #(apply store/bucket-put b  %)
;;    threads
;;    kvs))

;; (deftest ^{:performance true} bdb-cursor-test
;;   (ensure-test-directory)
;;   (let [b (bdb-bucket (bdb-db "bdb_test"
;;                                (bdb-env :path
;;                                         "/tmp/bdbtest")))]
;;     (load-bucket b (mk-data 10000 10000) 10)
;;     (time (count (bucket-seq b)))))