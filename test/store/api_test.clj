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
  (let [s (store [])]
    (s :add "b")
    (s :put "b" :k 42)
    (is (= 42
	   (s :get "b" :k)))))

(deftest store-add-test
  (let [s (store [])]
    (is (not (s :bucket "b")))
    (add-bucket s "b" (bucket {:type :mem}))
    (is (s :bucket "b"))
    (s :put "b" :k 42)
    (is (= 42
	   (s :get "b" :k)))))

(deftest start-flush-pools-test
  (let [s (store ["b"] {:merge (fn [_ sum x] (+ (or sum 0) x))
			:flush-freq 1})
	b (bucket-get (.bucket-map s) "b")]
    (s :merge "b" "k" 42)
    (Thread/sleep 2000)
    (is (= 42 (s :get "b" "k")))
    (shutdown s)
    (Thread/sleep 1000)
    (is (.isShutdown (:flush-pool b)))))

(deftest store-with-dynamic-bucket
  (let [s (store [{:name "foo"
		   :merge (fn [_ x y]
			    (+ (or x 0) (or y 0)))}]
		 {:type :mem})]
    (s :put "foo" :a 1)
    (s :add "bar")
    (s :put "bar" :b 2)

    (= 1 (s :get "foo" :a))
    (= 2 (s :get "bar" :b))))

(deftest store-clone-test
  (let [s (store ["foo" "bar"])]
    (s :put "foo" :a 1)
    (is (= ["foo" "bar"] (s :buckets)))
    (= 1 (s :get "foo" :a))
    (let [c (clone s)]
      (c :put "bar" :b 2)
      (= 2 (c :get "bar" :b))
      (= 1 (s :get "foo" :a)))))