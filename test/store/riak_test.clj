(ns store.riak-test
  (:use clj-json.core
	clojure.java.shell
        clojure.test
        store.riak
        store.core
	store.api
	plumbing.core
	plumbing.error
        [clj-time.coerce :only [to-date]]
        [clj-time.core :only [date-time now]]))

(deftest decode-chunked-objs-test
	 (let [bs [{"keys" ["foo"]}
		   {"keys" ["bar" "baz"]}
		   {"keys" []}]]
    (is (= ["foo" "bar" "baz"]
             (decode-chunked-objs bs)))))

(deftest parse-rfc2822-test
  (is (= (date-time 2011 3 3 5 55 57)
         (parse-rfc2822 "Thu, 03 Mar 2011 05:55:57 GMT"))))

(deftest get-riak-req-url-test
  (is (= "http://localhost:8098/riak/bucket"
         (get-riak-req-url {:name "bucket"} nil))) )

(deftest 
  riak-store-test
  (let [s (store ["b1","b2","b3"] {:type :riak})
	b (riak-bucket :name "b-key" :keywordize? true)]
    (bucket-put b "k" {:a 1})
    (is (= {:a 1} (bucket-get b "k")))
    (s :put "b1" "k" "v1")
    (is (nil? (s :get "b1" "not-found")))
    (is (= (s :get "b1" "k") "v1"))
    ;; test url encode
    (s :put "b1" "http://url.com" "v2")    
    (is (= (s :get "b1" "http://url.com")))
    (is (= (into #{} ["k" "http://url.com"])
	   (into #{} (s :keys "b1"))))
    (s :delete "b1" "k")
    (s :delete "b1" "http://url.com")
    (is (empty? (s :keys "b1")))
    (s :put "b2" "k2" {:a 1})
    (is (= [["k2" {"a" 1}]] (s :seq "b2")))
    (is (= [["k2" {"a" 1}]] (bucket-seq (s :bucket "b2"))))
    (is (= 1 ((s :get "b2" "k2") "a")))))
