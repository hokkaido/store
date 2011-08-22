(ns store.bdb-test
  (:use clojure.test
	store.api
	store.core
	store.bdb
	[store.core-test :only [generic-bucket-test]]
	[clojure.contrib.io :only [delete-file make-parents delete-file-recursively]]
	[plumbing.core :only [find-first]])
  (:import (com.sleepycat.je PreloadConfig)))

(defn test-bdb [& [args]]
  (bucket (merge args {:type :bdb
		       :name "bdb_test"
		       :path "/tmp/bdbtest/"})))

(defn new-test-bdb [& [args]]
  (delete-file-recursively (java.io.File. "/tmp/bdbtest") true)
  (make-parents (java.io.File. "/tmp/bdbtest/ping"))
  (delete-file (java.io.File.  "/tmp/bdbtest/ping") true)
  (test-bdb args))

(deftest bdb-basics
  (let [db (new-test-bdb)
	_ (bucket-put db :foo [1 2 3])]
    (is (= [1 2 3] (bucket-get db :foo)))
    (is (= [:foo [1 2 3]] (first (bucket-seq db))))
    (do (bucket-delete db :foo))
    (is (empty? (bucket-seq db)))
    (bucket-close db)))

(deftest bdb-read-only-test
  (let [db (new-test-bdb)]
    (bucket-put db "k" "v")
    (bucket-close db))
  (let [db-read (test-bdb {:read-only true})]
    (is (= "v" (bucket-get db-read "k")))
    (bucket-close db-read)))

(deftest bdb-deferred-write-test
  (let [db (new-test-bdb {:deferred-write true})]
    (bucket-put db "k" "v")
    (bucket-close db))
  (let [db-read (test-bdb {:deferred-write false})]
    (is (= "v" (bucket-get db-read "k")))
    (bucket-close db-read)))

(deftest bdb-bucket-test
  (let [db (new-test-bdb)]
    (generic-bucket-test db)
    (bucket-close db)))

(deftest bucket-keys-test
  (let [b (new-test-bdb)]
    (bucket-put b "k1" "v1")
    (bucket-put b "k2" "v2")
    (bucket-put b "k4" "v4")
    (is (= '("k1" "k2" "k4")
           (sort (bucket-keys b))))
    (bucket-close b)))

;;bdb can allow multiple vales for the same key, ensure we are operating in overwrite mode.
(deftest bdb-duplicate-puts
  (let [db (new-test-bdb)
	_ (bucket-put db :foo [1 2 3])
	_ (bucket-put db :foo [1])]
    (is (= [1] (bucket-get db :foo)))
    (is (= 1 (bucket-count db)))
    (do (bucket-delete db :foo))
    (is (empty? (bucket-seq db)))
    (bucket-close db)))

(deftest bdb-count
  (let [db (new-test-bdb)]
    (doseq [[k v] (partition-all 2 (range 100))] (bucket-put db k v))
    (is (= 50 (bucket-count db)))
    (bucket-close db)))