(ns store.bdb-test
  (:use clojure.test
	store.api
	store.bdb
	[store.core-test :only [generic-bucket-test]]
	[clojure.contrib.io :only [delete-file make-parents delete-file-recursively]]
	[plumbing.core :only [find-first]]))

(defn- ensure-test-directory []
  (delete-file-recursively (java.io.File. "/tmp/bdbtest") true)
  (make-parents (java.io.File. "/tmp/bdbtest/ping"))
  (delete-file (java.io.File.  "/tmp/bdbtest/ping") true))

(deftest bdb-basics
  (ensure-test-directory)
  (let [db (bdb-open "/tmp/bdbtest/" "bdb_test")
	_ (bdb-put db :foo [1 2 3])]
    (is (= [1 2 3] (bdb-get db :foo)))
    (is (= [:foo [1 2 3]] (first (entries-seq db))))
    (do (bdb-delete db :foo))
    (is (empty? (entries-seq db)))))

(deftest;; ^{:system true :bdb true}
    bdb-bucket-test
  (ensure-test-directory)
  (let [b (bdb-bucket "bdb_test" :env-path "/tmp/bdbtest")]
    (generic-bucket-test b)))