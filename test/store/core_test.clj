(ns store.core-test
  (:use clojure.test
	store.api))

(deftest basic-store-test
  (let [s (mk-store)]
    (s :put "b1" "k1" "v1")
    (is (= (s :get "b1" "k1") "v1"))
    (is (= ["k1"] (s :keys "b1")))))

(deftest asoc-store-test
  (let [root (java.io.File. ".")
	s (-> (mk-store) (assoc "fs" (fs-bucket (.getAbsolutePath root))))]
    (s :put "fs" "my-key" 2)
    (is (= (s :get "fs" "my-key") 2))
    (s :delete "fs" "my-key")))