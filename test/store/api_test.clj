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

(deftest bucket-test
  (let [b (bucket {:name "foo"
		   :type :mem
		   :merge (fn [_ x y] (+ (or x 0) (or y 0)))})]
    (bucket-merge b :a 2)
    (is (= 2 (bucket-get b :a)))))

(deftest build-merge-buckets
  (let [bs (buckets [{:name "foo"
		      :merge (fn [_ x y] (+ (or x 0) (or y 0)))}
		     "bar"]
		    {:type :mem})]
    (is (= ["foo" "bar"] (keys bs)))
    (bucket-merge (bs "foo") :a 1)
    (bucket-merge (bs "foo") :a 4)
    (= 5 (bucket-get (bs "foo") :a))))