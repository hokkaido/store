(ns store.daemon-test
  (:use clojure.test
        [store.api :only [hashmap-bucket bucket-put]]
        [plumbing.serialize :only [read-msg write-msg]]
        [plumbing.server :only [client start server]]
        [clojure.contrib.server-socket :only [close-server]])
  (:use store.daemon
	plumbing.serialize)
  (:import (java.net InetAddress)
           (java.io ByteArrayOutputStream)
           (org.apache.commons.io IOUtils)))

(deftest handler-test
  (let [baos (ByteArrayOutputStream.)]
    (server (bucket-server
	     {:b1 (doto (hashmap-bucket)
		    (bucket-put "k1" "val1"))
	      :b2 (hashmap-bucket)})
	    read-msg
	    write-msg
	    (IOUtils/toInputStream
	     "*3\r\n$3\r\nGET\r\n$2\r\nb1\r\n$4\r\n\"k1\"\r\n")
	    baos)
    (is (= "*1\r\n$6\r\n\"val1\"\r\n"
           (String. (.toByteArray baos))))))

(def test-client (partial client "127.0.0.1" 4444 (comp first read-msg) write-msg))

(deftest server-client-test
  (let [s (start
           (partial server (bucket-server
		    {:b1 (hashmap-bucket)
		     :b2 (hashmap-bucket)})
		     read-msg
		   write-msg)
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