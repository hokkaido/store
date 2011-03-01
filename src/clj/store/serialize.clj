(ns store.serialize)

(defprotocol Serializer
  "A protocol to convert between Clojure values
   and byte arrays."
  (^bytes serialize [this x])
  (deserialize [this ^bytes bs]))

(defn string-serializer
  [& {:keys [encoding]
      :or {encoding "UTF8"}}]
  (reify Serializer
         (serialize [this x]
                    (.getBytes (pr-str x) encoding))
         (deserialize [this bs]
                      (read-string (String. bs encoding)))))