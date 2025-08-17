import pandas as pd
from hdfs import InsecureClient
import psycopg2
import pymysql
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase
import json

# =========================
# Connexion HDFS
# =========================
HDFS_URL = "http://namenode:9870"
HDFS_USER = "hadoop"
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# =========================
# PostgreSQL
# =========================
def extract_postgresql(host, db, user, password):
    conn = psycopg2.connect(host=host, dbname=db, user=user, password=password)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema='public'
    """)
    tables = cursor.fetchall()

    for (table_name,) in tables:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        write_to_hdfs(df, f"postgresql/{table_name}")

    cursor.close()
    conn.close()

# =========================
# MySQL
# =========================
def extract_mysql(host, db, user, password):
    conn = pymysql.connect(host=host, database=db, user=user, password=password)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    for (table_name,) in tables:
        df = pd.read_sql(f"SELECT * FROM `{table_name}`", conn)
        write_to_hdfs(df, f"mysql/{table_name}")

    cursor.close()
    conn.close()

# =========================
# MongoDB
# =========================
def extract_mongodb(uri, db_name):
    client = MongoClient(uri)
    db = client[db_name]
    collections = db.list_collection_names()

    for coll in collections:
        data = list(db[coll].find())
        df = pd.DataFrame(data)
        write_to_hdfs(df, f"mongodb/{coll}")

# =========================
# Cassandra
# =========================
def extract_cassandra(contact_points, keyspace):
    cluster = Cluster(contact_points)
    session = cluster.connect(keyspace)
    tables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s", [keyspace])

    for row in tables:
        query = f"SELECT * FROM {row.table_name}"
        data = session.execute(query)
        df = pd.DataFrame(list(data))
        write_to_hdfs(df, f"cassandra/{row.table_name}")

    session.shutdown()
    cluster.shutdown()

# =========================
# Neo4j
# =========================
def extract_neo4j(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        labels = session.run("CALL db.labels()").data()
        for label in labels:
            label_name = label['label']
            result = session.run(f"MATCH (n:`{label_name}`) RETURN n")
            nodes = [record['n']._properties for record in result]
            df = pd.DataFrame(nodes)
            write_to_hdfs(df, f"neo4j/{label_name}")
    driver.close()

# =========================
# Fonction commune pour écrire dans HDFS
# =========================
def write_to_hdfs(df, path_prefix):
    csv_path = f"/data/{path_prefix}.csv"
    json_path = f"/data/{path_prefix}.json"

    # Écriture CSV
    with hdfs_client.write(csv_path, encoding='utf-8') as writer:
        df.to_csv(writer, index=False)

    # Écriture JSON
    with hdfs_client.write(json_path, encoding='utf-8') as writer:
        df.to_json(writer, orient='records', lines=True)

    print(f"✅ Données écrites : {csv_path} et {json_path}")

# =========================
# Exemple d'utilisation
# =========================
if __name__ == "__main__":
    # extract_postgresql("localhost", "education", "user", "password")
    extract_mysql("mysql-api", "dataflow360", "root", "1234")
    extract_mongodb("mongodb://root:root@localhost:27017", "datasetMongo")
    extract_mongodb("mongodb://root:root@localhost:27017", "education")
    extract_cassandra(["cassandra"], "ma_keyspace")
    extract_neo4j("bolt://localhost:7687", "neo4j", "12345678")
