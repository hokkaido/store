#Store

Store general key-value store api in Clojure.

It supports the following operations:

    :get
    :put
    :update
    :delete
    :exists?
    :keys

The api is currently backed but either s3 or Voldemort via Alex Feinberg's Clomert.  Special Thanks to Alex for writing the Clomert wrapper, as well as helping out with backing the Store api with Clomert.

We use a simple close-over-dependencies (dependency injection) style to create the stores, and you call the vatious store fns via a key argument into the store closure.

Suppose I have a store "s" I am putting tweets into - I just apply the store closure to the operation key, type (corresponds to an s3 bucket, or a Voldemort store), the value (a Clojure data structure), and the key (the user, in this case).

    (s :put "tweets" tweets user)

Let's see how the s3 and Voldemort cases are implemented.

Notice that when you call mk-store for s3, you can pass a map.  this let's you use simpler bucket keys, like "tweets" above, when the real s3 bucket name might be something crazy like my-project-name-user-tweets.

    (defn obj [s]
      (fn [op & args]
        (let [f (s op)]
          (apply f args))))

    (defn mk-store [s3 & m]
      (let [m (or m identity)]
        (obj {:put (fn [b v k]
             (try-default nil put-clj s3 (m b) (str k) v))
          :keys (fn [b]
              (try-default nil
                       get-keys s3 (m b)))

          :get (fn [b k]
             (try-default nil
                      get-clj s3 (m b) (str k)))
          :update (fn [b k]
                (try-default nil
                     append-clj s3 (m b) (str k)))
          :delete (fn [b k]
                (try-default nil
                     delete-object s3 (mb ) (str k)))

          :exists? (fn [b k]
                 (or
                  (some #(= k (.getKey %))
                    (try-default nil
                         (comp seq objects)
                         s3 (m b) (str k)))
                  false))})))

Although Voldemort and s3 are not the same semantically in all cases, you can back the above basic store api with both, as long as you are opinionated about conditional updates.  Looking at s3:

    (defn update* [bucket s3 k]
      (try-default nil
       append-clj s3 bucket (str k)))

append clj in s3.clj is just:

    (defn append-clj [s3 bucket-name key data]
      (let [old (get-clj s3 bucket-name key)]
        (put-clj s3 bucket-name key (append [old data]))))

So this is a non-transactional operation, and it is not an efficient operation built into the underlying store.  We could make it transactional (I think), but we don't care about that level of robustness yet for the data store storing in the KV stores.  We simply use the append function from store.core:

    (defn append
    "takes a seq of two elements, updaing the first element with the second element, depending on their types.

    concating/merging updates, not one-at-a-time-updates.

    i.e. [[1 2] [3]] -> [1 2 3]

    not [[12] 3] -> [1 2 3]
    "
    [xs]
      (let [root (first xs)]
        (cond
         (string? root) (apply str xs)
         (map? root) (apply merge xs)
         (vector? root) (vec (apply concat xs))
         :else (apply concat xs))))

This is the same approach we use in Voldemort:

    (defn mk-vstore
      [stores]
      (obj {:put (fn [bucket k v]
               (v/do-store
            (stores (str bucket))
            (:put k v)))
        :get (fn [bucket k]
               (v/versioned-value (v/do-store
                       (stores (str bucket))
                       (:get k))))
        :update (fn [bucket k v]
              (v/store-apply-update
               (stores (str bucket))
               (fn [client]
                 (let [ver (v/store-get client k)
                   val (v/versioned-value ver)]
                   (v/store-conditional-put client
                            k
                            (v/versioned-set-value! ver (append
                                             [v
                                             val])))))))
        :delete (fn [bucket k]
              (v/do-store
               (stores (str bucket))
               (:delete k)))}))

But one noticeable difference is that, for Voldemort, we use a cache of stores in the same way we use s3's notion of a bucket.

    (mk-vstore (mk-store-cache {:bootstrap-urls "tcp://localhost:6666"}))

    (defn mk-store-cache [config]
      (let [factory (v/make-socket-store-client-factory
             (v/make-client-config config))
        m (java.util.concurrent.ConcurrentHashMap.)]
        (fn [client]
          (if-let [c (get m client)]
        c
        (let [c (v/make-store-client factory client)]
          (.put m client c)
          c)))))