(ns store.serialize-test
  (:use clojure.test
        store.serialize))

(deftest test-string-serializer
  (let [ss (string-serializer :encoding "UTF8")]
    (is (= [1 2 (list 4 5) {:a 42}]
             (deserialize ss
                          (serialize ss
                                     [1 2 (list 4 5) {:a 42}]))))))