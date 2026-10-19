;; Execute the bundled JDBC drivers from the pinned Metabase image. This does
;; not invoke Metabase's query builder; it captures JDBC setup and metadata SQL.
(require '[cheshire.core :as json])
(import '(java.sql DriverManager SQLException ResultSet)
        '(java.util Properties))

(defn result [^ResultSet rows]
  (with-open [rows rows]
    (let [metadata (.getMetaData rows)
          columns (mapv #(.getColumnLabel metadata %) (range 1 (inc (.getColumnCount metadata))))]
      {:columns columns
       :rows (loop [values []]
               (if (and (< (count values) 64) (.next rows))
                 (recur (conj values (mapv #(some-> (.getObject rows (int %)) str)
                                          (range 1 (inc (count columns))))))
                 values))})))

(defn attempt [name operation]
  (try (assoc (operation) :operation name :passed true)
       (catch SQLException error
         {:operation name :passed false :sqlstate (.getSQLState error) :error (.getMessage error)})))

(defn metabase-query [connection [sql & arguments]]
  (with-open [statement (.prepareStatement connection sql)]
    (doseq [[i value] (map-indexed vector arguments)]
      (.setString statement (int (inc i)) value))
    (result (.executeQuery statement))))

(defn snowflake-values [connection]
  (with-open [statement (.createStatement connection)
              rows (.executeQuery statement "SELECT AMOUNT, \"MixedCase\", PAYLOAD, CREATED FROM ANALYTICS.METADATA_TYPES")]
    (when-not (.next rows) (throw (SQLException. "type-coverage row is missing")))
    {:decimal (.toPlainString (.getBigDecimal rows 1))
     :null (do (.getString rows 2) (.wasNull rows))
     :binary (vec (.getBytes rows 3))
     :timestamp (str (.toLocalDateTime (.getTimestamp rows 4)))}))

(defn metabase-sync [connection schema table]
  (require 'metabase.driver.redshift)
  (let [tables ((ns-resolve 'metabase.driver.redshift 'get-tables-sql) nil)
        fields ((requiring-resolve 'metabase.driver.sql-jdbc.sync/describe-fields-sql)
                :redshift :schema-names [schema] :table-names [table])]
    [(attempt "metabase_tables" #(metabase-query connection tables))
     (attempt "metabase_fields" #(metabase-query connection fields))]))

(let [profile (System/getenv "YASP_JDBC_PROFILE")
      schema (or (System/getenv "YASP_JDBC_SCHEMA") "public")
      table (or (System/getenv "YASP_JDBC_TABLE") "users")
      driver (if (= profile "redshift") "com.amazon.redshift.jdbc.Driver" "org.postgresql.Driver")
      scheme (if (= profile "redshift") "redshift" "postgresql")
      properties (doto (Properties.)
                   (.setProperty "user" (System/getenv "YASP_JDBC_USER"))
                   (.setProperty "password" (System/getenv "YASP_JDBC_PASSWORD"))
                   (.setProperty "ApplicationName" "YASP compatibility")
                   (.setProperty "preferQueryMode" "simple")
                   (.setProperty "compression" "off")
                   (.setProperty "sslmode" (if (= profile "redshift") "verify-ca" "require"))
                   (.setProperty "sslrootcert" "/tmp/yasp-fixture-ca.crt")
                   (.setProperty "loginTimeout" "5")
                   (.setProperty "socketTimeout" "5"))
      url (str "jdbc:" scheme "://" (System/getenv "YASP_JDBC_HOST") ":"
               (System/getenv "YASP_JDBC_PORT") "/" (System/getenv "YASP_JDBC_DATABASE"))]
  (Class/forName driver)
  (try
    (with-open [connection (DriverManager/getConnection url properties)]
      (let [metadata (.getMetaData connection)
            observations (into [(attempt "schemas" #(result (.getSchemas metadata)))
                          (attempt "tables" #(result (.getTables metadata nil schema table (into-array String ["TABLE" "VIEW"]))))
                          (attempt "columns" #(result (.getColumns metadata nil schema table "%")))
                          (attempt "primary_keys" #(result (.getPrimaryKeys metadata nil schema table)))
                          (attempt "foreign_keys" #(result (.getImportedKeys metadata nil schema (if (= schema "PUBLIC") "ORDERS" "orders"))))
                          (attempt "indexes" #(result (.getIndexInfo metadata nil schema table false false)))
                          (attempt "preview" #(with-open [statement (.createStatement connection)]
                                                (result (.executeQuery statement "SELECT id, full_name FROM public.users ORDER BY id LIMIT 5"))))
                          (attempt "transaction" #(do (.setReadOnly connection true)
                                                       (.setAutoCommit connection false)
                                                       (with-open [statement (.createStatement connection)]
                                                         (result (.executeQuery statement "SELECT count(*) FROM public.users")))
                                                       (.rollback connection)
                                                       (.setAutoCommit connection true)
                                                       {:rolled_back true}))]
                               (concat (when (= profile "redshift") (metabase-sync connection schema table))
                                       (when (= schema "PUBLIC")
                                         [(attempt "native_values" #(snowflake-values connection))
                                          (attempt "composite_primary_keys" #(result (.getPrimaryKeys metadata nil "ANALYTICS" "METADATA_TYPES")))
                                          (attempt "composite_foreign_keys" #(result (.getImportedKeys metadata nil "ANALYTICS" "TYPE_REFERENCES")))])))]
        (println (str "RESULT " (json/generate-string {:profile profile :connected true
                                                      :driver_version (.getDriverVersion metadata)
                                                      :observations observations})))))
    (catch SQLException error
      (println (str "RESULT " (json/generate-string {:profile profile :connected false
                                                    :sqlstate (.getSQLState error) :error (.getMessage error)
                                                    :causes (mapv #(str (.getName (class %)) ": " (.getMessage %)) (take 8 (take-while some? (iterate #(.getCause %) error))))
                                                    :frames (mapv str (take 8 (.getStackTrace error)))}))))))
(shutdown-agents)
