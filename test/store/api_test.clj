(ns store.api-test
  (:use clojure.test
	store.api
	store.core))

(deftest build-memory-buckets
  (let [bs (buckets ["foo"
		     {:name "bar"}]
		    {:type :mem})]
    (is (= ["foo" "bar"] (keys bs)))
    (bucket-put (bs "foo") :a 1)
    (= 1 (bucket-get (bs "foo") :a))))

