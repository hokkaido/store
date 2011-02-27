(ns store.riak-test
  (:use clj-json.core clojure.test store.riak))

(deftest decode-json-bodys-test
  (let [bs (map generate-string
		[{"keys" ["foo"]}
		 {"keys" ["bar" "baz"]}
		 {"keys" []}])]
    (is (= ["foo" "bar" "baz"]
	     (decode-json-bodys bs)))))