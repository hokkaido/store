# Store 

## Basics

Store general key-value store api in Clojure. The goal of store is to abstract away from policies about how data is stored and provide a uniform API for application data. A store consists of many buckets and each bucket has it's own key-value namespace. A store operation takes the form 

  (store operation bucket-name & args)

where operation is the name of a bucket operation and args are any other args needed for the operations. The following are valid store operations:

    * :get  - return value associated with a given key in a bucket <code>(s :get "bucket" "key") </code>
    * :put  - store value associated with a given key in a bucket <code>(s :put "bucket" "key" "val")</code> 
    * :update - update value associated with a key using a function <code>(s :update "visitor-counts" "user" (fnil inc 0)</code>
    * :merge - a special case of update for a merging function specific to the bucket specified at bucket-declaration <code>(s :merge "visior-counts" "user" 1)</code>.  
    * :delete - delete a key-value pair <code>(s :delete "bucket" "key")</code>.
    * :exists? - does a given key exist? (s :exists? "bucket" "key")
    * :keys - return seq of bucket keys
    * :seq - return seq of [k v] pairs in bucket
    * :sync - when using a bucket with flush (see below) sync in-memory state to underlying bucket
   
## Specifying a Store

You specify a store by specifying a collection of buckets. Here's an example

  (def bucket-specs
    [{:name "tweets"
      :type :fs
      :path "/tmp/data/tweets"
      :merge (fn [key old-tweets new-tweets] (concat old-tweets new-tweets))}
     {:name "session"
      :type :mem}
     {:name "visit-statistics"
      :type :bdb
      :merge (fn [key cur-stats new-stats] (merge-with + cur-stats new-stats))
      :flush [:self]
      :flush-freq 30}
     {:name "oauth-tokens"
      :type :rest
      :host "http://my-store-server.com"
      :port 6660}])

    (def s (store bucket-specs {:db-env berkeley-db-env})

In this example "tweets" is stored on the local filesystem at a path, where filenames are keys and contents are values. We specify a merge function to be used on this bucket with the <code>:merge</code> keyword. The visit-statistics bucket uses the `flush' mechanism which allows writes in the process to be accumulated to an in-memory hash-map and periodically flushed to the underlying store (which in this case is Berkeley BDB Java edition) every 30 seconds. The flush buckets support atomic merges with the specified merge function. In general however, store oeprations are  non-transactional operation.

##  Store over the network

In <code>store.net</code> you can trivially expose a store over a REST interface (using Jetty as the underlying server). The REST api for accessing the store looks like this

   curl http://my-store-host.com/store/get/bucket/key

Or if you want to merge values to a store across the network 

   curl -X POST -d '{"status": "I love Dr. Who"}' http://my-store-host.com/store/merge/tweets/user

to merge the given tweet with the rest of the tweets without `pulling down' all tweets from the store. This is useful for writing services that need to aggregate data structures without seeing the whole object. 



## Sponsors

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).