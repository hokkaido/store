(ns store.api-test
  (:use clojure.test
	store.api
	store.core))

(deftest build-memory-buckets
  (let [s (store [{:name "foo"}]
		    {:type :mem})]
    (s :put "foo" :a 1)
    (= 1 (s :get "foo" :a))))

(deftest bucket-test
  (let [s (store [{:name "foo"
		    :type :mem
		    :merge (fn [_ x y] (+ (or x 0) (or y 0)))}])]
    (s :merge "foo" :a 2)
    (is (= 2 (s :get "foo" :a)))))

(deftest build-merge-buckets
  (let [s (store [{:name "foo"
		   :merge (fn [_ x y] (+ (or x 0) (or y 0)))}]
		 {:type :mem})]
    (s :merge "foo" :a 1)
    (s :merge "foo" :a 4)
    (= 5 (s :get "foo" :a))))

(deftest store-op-test
  (is (= 42 (store-op {:k {:read {:k 42}}}
		      :k :get :k))))