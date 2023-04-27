(ns db-syncer.specs
  (:require [clojure.spec.alpha :as s]))

(def db-types? #{:postgres :sqlite :mysql :sqlserver})

(s/def ::db-type db-types?)
(s/def ::db-url string?)

(s/def ::db-spec (s/keys :req-un [::db-type ::db-url]))
