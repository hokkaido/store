(ns store.riak-test
  (:use clj-json.core
        clojure.test
        store.riak
        store.api
        
        [clj-time.coerce :only [to-date]]
        [clj-time.core :only [date-time now]]))

(deftest decode-json-bodys-test
  (let [bs (map generate-string
		[{"keys" ["foo"]}
		 {"keys" ["bar" "baz"]}
		 {"keys" []}])]
    (is (= ["foo" "bar" "baz"]
             (decode-json-bodys bs)))))

(deftest parse-rfc2822-test
  (is (= (date-time 2011 3 3 5 55 57)
         (parse-rfc2822 "Thu, 03 Mar 2011 05:55:57 GMT"))))

;; TODO: setup a bucket
#_(deftest ^{:system true} bucket-modified-test
  (is (nil? (bucket-modified b
                             "this-key-doesnt-exist")))
  (is (before? (now)
               (bucket-modified b
                                "this-key-exists"))))