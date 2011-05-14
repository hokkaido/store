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

(deftest store-final-flush-test
  (let [s (store
	   [{:name "b2"
	     :merge (fn [_ sum x] (+ (or sum 0) x))
	     :flush (fn [_ sum x] (+ (or sum 0) x))}])]
    (s :merge "b2" "k" 42)
    (is (nil? (s :get "b2" "k")))
    (s :sync "b2")
    (is (= 42 (s :get "b2" "k")))))

(deftest store-op-test
  (is (= 42
	 (store-op {:k {:read {:k 42}}}
		   :get :k :k))))

(deftest start-flush-pools-test
  (let [b (with-merge
	     (hashmap-bucket)
	     (fn [_ sum x] (+ (or sum 0) x)))
	bs (start-flush-pools
	    {"b" {:write-spec {:flush (fn [_ sum x] (+ (or sum 0) x)) :flush-freq 2}
		  :write b
		  :read b}})
	s (store.api.Store. bs)]
    (s :merge "b" "k" 42)
    (Thread/sleep 2000)
    (is (= 42 (s :get "b" "k")))
    (shutdown s)
    (Thread/sleep 1000)
    (is (.isShutdown (:flush-pool (bs "b"))))))