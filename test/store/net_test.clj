(ns store.net-test
  (:use clojure.test
        [plumbing.server :only [start server client]]
        [plumbing.serialize :only [read-str-msg write-str-msg reader writer]]
        [clojure.contrib.server-socket :only [close-server]]
	store.api
        store.net)
  (:import (java.net InetAddress)
           (java.io ByteArrayOutputStream)
           (org.apache.commons.io IOUtils))
  (:require [store.api :as store]
            [store.bdb :as bdb]))

;; (deftest handler-test
;;   (let [baos (ByteArrayOutputStream.)]
;;     (server (bucket-server
;; 	     {:b1 (doto (hashmap-bucket)
;; 		    (bucket-put "k1" "val1"))
;; 	      :b2 (hashmap-bucket)})
;; 	    read-msg
;; 	    write-msg
;; 	    (IOUtils/toInputStream
;; 	     "*3\r\n$3\r\nGET\r\n$2\r\nb1\r\n$4\r\n\"k1\"\r\n")
;; 	    baos)
;;     (is (= "*1\r\n$6\r\n\"val1\"\r\n"
;;            (String. (.toByteArray baos))))))

(def test-client (partial client "127.0.0.1" 4444
			  (comp first read-str-msg reader)
			  (fn [s msg]
			    (write-str-msg (writer s) msg))))

(deftest server-client-test
  (let [s (start
           (partial server (bucket-server
		    {:b1 (hashmap-bucket)
		     :b2 (hashmap-bucket)})
		    (comp read-str-msg reader)
		    (fn [s msg]
		      (write-str-msg (writer s) msg)))
           :port 4444)]

    (is (= nil
           (test-client ["PUT" "b1"
			       "key1" "val1"])))
    (is (= "val1"
           (test-client ["GET" "b1" "key1"])))
    (is (= nil
           (test-client ["PUT" "b1"
			       "key2" "val2"])))
    (is (= #{"key1" "key2"}
           (set (test-client ["KEYS" "b1"]))))
    (is (= '(["key2" "val2"] ["key1" "val1"])
           (test-client ["SEQ" "b1"])))
    (is (= "val1"
           (test-client ["DELETE" "b1" "key1"])))
    (is (= "val2"
           (test-client ["DELETE" "b1" "key2"])))

    (test-client ["PUT" "b1" "http://aria42.com" "v"])
    (is (= "v" (test-client ["GET" "b1" "http://aria42.com"])))
    (close-server s)))

(deftest get-put-test
  (let [s (start
           (partial server (bucket-server
                            {:b1 (store/hashmap-bucket)})
                    (comp read-str-msg reader)
		    (fn [s msg]
		       (write-str-msg (writer s) msg)))
           :port 4444)
        b (net-bucket :name "b1"
                      :host "127.0.0.1"
                      :port 4444)]
    (is (nil? (store/bucket-get b "k1")))
    (store/bucket-put b "k1" "v1")
    (is (= "v1"
           (store/bucket-get b "k1")))
    (store/bucket-put b "k2" 5)
    (is (= 5
           (store/bucket-get b "k2")))
    (store/bucket-put b
                      "k3"
                      {:a1 {:b11 "b11val" :b12 12}
                       :a2 {:b21 "b21val" :b22 22}})
    (is (= {:a1 {:b11 "b11val" :b12 12}
            :a2 {:b21 "b21val" :b22 22}}
           (store/bucket-get b "k3")))

    (close-server s)))

(deftest bdb-server-test
  (let [db-env (bdb/bdb-env :path "/tmp/nettest"
                            :cache-percent 35
                            :clean-util-thresh 75
                            :checkpoint-high-priority? true
                            :num-cleaner-threads 3
                            :locking true)
        s (start
           (partial server
		    (bucket-server
		     {:b1 (bdb/bdb-bucket
			   (bdb/bdb-db "b1" db-env
				       :cache-mode :evict-ln))})
                    (comp read-str-msg reader)
		    (fn [s msg]
		      (write-str-msg (writer s) msg)))
           :port 4445)
        b (net-bucket :name "b1"
                      :host "127.0.0.1"
                      :port 4445)]
    (is (nil?
         (store/bucket-get b "k1")))

    (store/bucket-put b "k1" {:foo 0.99})

    (is (= {:foo 0.99}
           (store/bucket-get b "k1")))

    (close-server s)))
