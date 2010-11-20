(ns store.core-test
  (:use clojure.test
	store.api))

(deftest basic-store-test
  (let [s (mk-store)]
    (s :put "b1" "k1" "v1")
    (is (= (s :get "b1" "k1") "v1"))
    (is (= ["k1"] (s :keys "b1")))))