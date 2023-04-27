(ns db-syncer.core
  (:require
   [clj-clapps.core :refer [defcmd exec! exit]]
   [db-syncer.db :as db]
   [db-syncer.specs :as s]))

(defcmd sync-cmd
  "db-sync"
  [^{:doc "source table"} source-table
   ^{:doc "Dest table"} dest-table
   & [^{:doc "source db URL, defaults to environment variable DB_SYNC_SRC_DB"
        :env "DB_SYNC_SRC_DB"
        :parse-fn db/db-spec
        :short "-s"} source-db
      ^{:doc "dest db URL, defaults to environment variable DB_SYNC_DEST_DB"
        :env "DB_SYNC_DEST_DB"
        :parse-fn db/db-spec
        :short "-d"} dest-db]]
  (when (nil? source-db)
    (exit 1 "missing SOURCE_DB"))
  (when (nil? dest-db)
    (exit 1 "missing DEST_DB"))
  (as-> (:dbtype dest-db) db-type
    (when-not (s/db-types? db-type)
      (exit 1 (str "invalid or unsupported db-type:" db-type))))
  (as-> (:dbtype dest-db) db-type
    (when-not (s/db-types? db-type)
      (exit 1 (str "invalid or unsupported db-type:" db-type))))
  (db/sync-tables! source-db source-table dest-db dest-table))

(defn -main [& args]
  (exec! 'db-syncer.core args))
