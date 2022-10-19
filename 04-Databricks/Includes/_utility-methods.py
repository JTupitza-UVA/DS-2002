# Databricks notebook source
# Debugging
# username = spark.sql("SELECT current_user()").first()[0]
# path = f"dbfs:/user/{username}/dbacademy/dewd/"
# print(f"Nuking {path}")
# dbutils.fs.rm(path, True)

# COMMAND ----------

class Paths():
    def __init__(self, working_dir, clean_lesson):
        self.working_dir = working_dir

        if clean_lesson: self.user_db = f"{working_dir}/{clean_lesson}.db"
        else:            self.user_db = f"{working_dir}/database.db"
            
    def exists(self, path):
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def print(self, padding="  "):
        max_key_len = 0
        for key in self.__dict__: max_key_len = len(key) if len(key) > max_key_len else max_key_len
        for key in self.__dict__:
            label = f"{padding}DA.paths.{key}:"
            print(label.ljust(max_key_len+13) + DA.paths.__dict__[key])

class DBAcademyHelper():
    def __init__(self, lesson=None):
        import re, time

        self.start = int(time.time())
        
        self.course_name = "dewd"
        self.lesson = lesson.lower()
        self.data_source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/data-engineering-with-databricks/v02"

        # Define username
        self.username = spark.sql("SELECT current_user()").first()[0]
        self.clean_username = re.sub("[^a-zA-Z0-9]", "_", self.username)

        self.db_name_prefix = f"dbacademy_{self.clean_username}_{self.course_name}"
        self.source_db_name = None

        self.working_dir_prefix = f"dbfs:/user/{self.username}/dbacademy/{self.course_name}"
        
        if self.lesson:
            clean_lesson = re.sub("[^a-zA-Z0-9]", "_", self.lesson)
            working_dir = f"{self.working_dir_prefix}/{self.lesson}"
            self.paths = Paths(working_dir, clean_lesson)
            self.hidden = Paths(working_dir, clean_lesson)
            self.db_name = f"{self.db_name_prefix}_{clean_lesson}"
        else:
            working_dir = self.working_dir_prefix
            self.paths = Paths(working_dir, None)
            self.hidden = Paths(working_dir, None)
            self.db_name = self.db_name_prefix

    def init(self, create_db=True):
        spark.catalog.clearCache()
        self.create_db = create_db
        
        if create_db:
            print(f"\nCreating the database \"{self.db_name}\"")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.paths.user_db}'")
            spark.sql(f"USE {self.db_name}")

    def cleanup(self):
        for stream in spark.streams.active:
            print(f"Stopping the stream \"{stream.name}\"")
            stream.stop()
            try: stream.awaitTermination()
            except: pass # Bury any exceptions

        if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database \"{self.db_name}\"")
            spark.sql(f"DROP DATABASE {self.db_name} CASCADE")
            
        if self.paths.exists(self.paths.working_dir):
            print(f"Removing the working directory \"{self.paths.working_dir}\"")
            dbutils.fs.rm(self.paths.working_dir, True)

    def conclude_setup(self):
        import time

        spark.conf.set("da.username", self.username)
        spark.conf.set("da.db_name", self.db_name)
        for key in self.paths.__dict__:
            spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])

        print("\nPredefined Paths:")
        DA.paths.print()

        if self.source_db_name:
            print(f"\nPredefined tables in {self.source_db_name}:")
            tables = spark.sql(f"SHOW TABLES IN {self.source_db_name}").filter("isTemporary == false").select("tableName").collect()
            if len(tables) == 0: print("  -none-")
            for row in tables: print(f"  {row[0]}")

        if self.create_db:
            print(f"\nPredefined tables in {self.db_name}:")
            tables = spark.sql(f"SHOW TABLES IN {self.db_name}").filter("isTemporary == false").select("tableName").collect()
            if len(tables) == 0: print("  -none-")
            for row in tables: print(f"  {row[0]}")
                
        print(f"\nSetup completed in {int(time.time())-self.start} seconds")
        
    def block_until_stream_is_ready(self, query, min_batches=2):
        import time
        while len(query.recentProgress) < min_batches:
            time.sleep(5) # Give it a couple of seconds

        print(f"The stream has processed {len(query.recentProgress)} batchs")
        
dbutils.widgets.text("lesson", "missing")
lesson = dbutils.widgets.get("lesson")
if lesson == "none": lesson = None
assert lesson != "missing", f"The lesson must be passed to the DBAcademyHelper"

DA = DBAcademyHelper(lesson)

# COMMAND ----------

def install_source_dataset(source_uri, reinstall, subdir):
    target_dir = f"{DA.working_dir_prefix}/source/{subdir}"

#     if reinstall and DA.paths.exists(target_dir):
#         print(f"Removing existing dataset at {target_dir}")
#         dbutils.fs.rm(target_dir, True)
    
    if DA.paths.exists(target_dir):
        print(f"Skipping install to \"{target_dir}\", dataset already exists")
    else:
        print(f"Installing datasets to \"{target_dir}\"")
        dbutils.fs.cp(source_uri, target_dir, True)
        
    return target_dir

# COMMAND ----------

def install_dtavod_datasets(reinstall):
    source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/databases_tables_and_views_on_databricks/v02"
    DA.paths.datasets = install_source_dataset(source_uri, reinstall, "dtavod")

    copy_source_dataset(f"{DA.paths.datasets}/flights/departuredelays.csv", 
                        f"{DA.paths.working_dir}/flight_delays",
                        format="csv", name="flight_delays")

# COMMAND ----------

def install_eltwss_datasets(reinstall):
    source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/elt-with-spark-sql/v02/small-datasets"
    DA.paths.datasets = install_source_dataset(source_uri, reinstall, "eltwss")

# COMMAND ----------

def clone_source_table(table_name, source_path, source_name=None):
    import time
    start = int(time.time())

    source_name = table_name if source_name is None else source_name
    print(f"Cloning the {table_name} table from {source_path}/{source_name}", end="...")
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name}
        SHALLOW CLONE delta.`{source_path}/{source_name}`
        """)

    total = spark.read.table(table_name).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
def load_eltwss_tables():
    clone_source_table("events", f"{DA.paths.datasets}/delta")
    clone_source_table("sales", f"{DA.paths.datasets}/delta")
    clone_source_table("users", f"{DA.paths.datasets}/delta")
    clone_source_table("transactions", f"{DA.paths.datasets}/delta")    

# COMMAND ----------

def copy_source_dataset(src_path, dst_path, format, name):
    import time
    start = int(time.time())
    print(f"Creating the {name} dataset", end="...")
    
    dbutils.fs.cp(src_path, dst_path, True)

    total = spark.read.format(format).load(dst_path).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
def load_eltwss_external_tables():
    copy_source_dataset(f"{DA.paths.datasets}/raw/sales-csv", 
                        f"{DA.paths.working_dir}/sales-csv", "csv", "sales-csv")

    import time
    start = int(time.time())
    print(f"Creating the users table", end="...")

    # REFACTORING - Making lesson-specific copy
    dbutils.fs.cp(f"{DA.paths.datasets}/raw/users-historical", 
                  f"{DA.paths.working_dir}/users-historical", True)

    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    (spark.read
          .format("parquet")
          .load(f"{DA.paths.working_dir}/users-historical")
          .repartition(1)
          .write
          .format("org.apache.spark.sql.jdbc")
          .option("url", f"jdbc:sqlite:/{DA.username}_ecommerce.db")
          .option("dbtable", "users") # The table name in sqllight
          .mode("overwrite")
          .save())

    total = spark.read.parquet(f"{DA.paths.working_dir}/users-historical").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

# lesson: Writing delta 
def create_eltwss_users_update():
    import time
    start = int(time.time())
    print(f"Creating the users_dirty table", end="...")

    # REFACTORING - Making lesson-specific copy
    dbutils.fs.cp(f"{DA.paths.datasets}/raw/users-30m", 
                  f"{DA.paths.working_dir}/users-30m", True)
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE users_dirty AS
        SELECT *, current_timestamp() updated 
        FROM parquet.`{DA.paths.working_dir}/users-30m`
    """)
    
    spark.sql("INSERT INTO users_dirty VALUES (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL)")
    
    total = spark.read.table("users_dirty").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

class DltDataFactory:
    def __init__(self):
        self.source = f"/mnt/training/healthcare/tracker/streaming"
        self.userdir = f"{DA.paths.working_dir}/source/tracker"
        try:
            self.curr_mo = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.userdir)]))
        except:
            self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.userdir}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.userdir}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1
