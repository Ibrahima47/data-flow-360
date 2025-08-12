import csv
import json
from hdfs import InsecureClient
import psycopg2
import pymongo
import mysql.connector
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

# ========================
# 1. Connexion HDFS
# ========================
hdfs_client = InsecureClient("http://namenode:9870", user="hadoop")

# ========================
# 2. PostgreSQL (stream)
# ========================
def export_postgresql_to_hdfs_csv_json(host, dbname, user, password, hdfs_dir="/data/postgre", batch_size=10000):
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()

    for (table_name,) in tables:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Récupérer colonnes
        columns = [desc[0] for desc in cursor.description]

        # CSV
        with hdfs_client.write(f"{hdfs_dir}/{table_name}.csv", encoding="utf-8") as writer:
            csv_writer = csv.writer(writer)
            csv_writer.writerow(columns)

            cursor.execute(f"SELECT * FROM {table_name}")
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                csv_writer.writerows(rows)

        # JSON
        with hdfs_client.write(f"{hdfs_dir}/{table_name}.json", encoding="utf-8") as writer:
            cursor.execute(f"SELECT * FROM {table_name}")
            first = True
            writer.write("[")
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    if not first:
                        writer.write(",")
                    writer.write(json.dumps(dict(zip(columns, row))))
                    first = False
            writer.write("]")

    cursor.close()
    conn.close()

# ========================
# 3. MongoDB (stream)
# ========================
def export_mongodb_to_hdfs_csv_json(uri, dbname, hdfs_dir="/data/mongoDB", batch_size=10000):
    client = pymongo.MongoClient(uri)
    db = client[dbname]

    for collection_name in db.list_collection_names():
        collection = db[collection_name]

        # CSV
        with hdfs_client.write(f"{hdfs_dir}/{collection_name}.csv", encoding="utf-8") as writer:
            cursor = collection.find({}, batch_size=batch_size)
            first_doc = next(cursor, None)
            if not first_doc:
                continue
            keys = list(first_doc.keys())
            csv_writer = csv.writer(writer)
            csv_writer.writerow(keys)
            csv_writer.writerow([first_doc.get(k, "") for k in keys])
            for doc in cursor:
                csv_writer.writerow([doc.get(k, "") for k in keys])

        # JSON
        with hdfs_client.write(f"{hdfs_dir}/{collection_name}.json", encoding="utf-8") as writer:
            cursor = collection.find({}, batch_size=batch_size)
            writer.write("[")
            first = True
            for doc in cursor:
                if not first:
                    writer.write(",")
                writer.write(json.dumps(doc, default=str))
                first = False
            writer.write("]")

    client.close()

# ========================
# 4. MySQL (stream)
# ========================
def export_mysql_to_hdfs_csv_json(host, user, password, database, hdfs_dir="/data/mysql", batch_size=10000):
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    for (table_name,) in tables:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        columns = [desc[0] for desc in cursor.description]

        # CSV
        with hdfs_client.write(f"{hdfs_dir}/{table_name}.csv", encoding="utf-8") as writer:
            csv_writer = csv.writer(writer)
            csv_writer.writerow(columns)
            cursor.execute(f"SELECT * FROM {table_name}")
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                csv_writer.writerows(rows)

        # JSON
        with hdfs_client.write(f"{hdfs_dir}/{table_name}.json", encoding="utf-8") as writer:
            cursor.execute(f"SELECT * FROM {table_name}")
            first = True
            writer.write("[")
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    if not first:
                        writer.write(",")
                    writer.write(json.dumps(dict(zip(columns, row))))
                    first = False
            writer.write("]")

    cursor.close()
    conn.close()

# ========================
# 5. Cassandra (stream)
# ========================
def export_cassandra_to_hdfs_csv_json(hosts, keyspace, hdfs_dir="/data/cassandra", batch_size=10000):
    cluster = Cluster(hosts)
    session = cluster.connect(keyspace)

    tables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s", [keyspace])
    for row in tables:
        table_name = row.table_name
        columns_info = session.execute(f"SELECT column_name FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s", [keyspace, table_name])
        columns = [col.column_name for col in columns_info]

        query = f"SELECT * FROM {table_name}"
        result = session.execute(query, fetch_size=batch_size)

        # CSV
        with hdfs_client.write(f"{hdfs_dir}/{table_name}.csv", encoding="utf-8") as writer:
            csv_writer = csv.writer(writer)
            csv_writer.writerow(columns)
            for row in result:
                csv_writer.writerow([getattr(row, col) for col in columns])

        # JSON
        result = session.execute(query, fetch_size=batch_size)
        with hdfs_client.write(f"{hdfs_dir}/{table_name}.json", encoding="utf-8") as writer:
            writer.write("[")
            first = True
            for row in result:
                if not first:
                    writer.write(",")
                writer.write(json.dumps({col: getattr(row, col) for col in columns}, default=str))
                first = False
            writer.write("]")

    cluster.shutdown()

# ========================
# 6. Neo4j (stream)
# ========================
def export_neo4j_to_hdfs_csv_json(uri, user, password, hdfs_dir="/data/neo4j"):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        # Export des noeuds
        nodes = session.run("MATCH (n) RETURN labels(n) AS labels, properties(n) AS props")
        with hdfs_client.write(f"{hdfs_dir}/nodes.csv", encoding="utf-8") as writer:
            csv_writer = csv.writer(writer)
            csv_writer.writerow(["labels", "properties"])
            for record in nodes:
                csv_writer.writerow([record["labels"], record["props"]])

        with hdfs_client.write(f"{hdfs_dir}/nodes.json", encoding="utf-8") as writer:
            writer.write("[")
            first = True
            nodes = session.run("MATCH (n) RETURN labels(n) AS labels, properties(n) AS props")
            for record in nodes:
                if not first:
                    writer.write(",")
                writer.write(json.dumps({"labels": record["labels"], "properties": record["props"]}, default=str))
                first = False
            writer.write("]")

        # Export des relations
        rels = session.run("MATCH ()-[r]->() RETURN type(r) AS type, properties(r) AS props")
        with hdfs_client.write(f"{hdfs_dir}/relationships.csv", encoding="utf-8") as writer:
            csv_writer = csv.writer(writer)
            csv_writer.writerow(["type", "properties"])
            for record in rels:
                csv_writer.writerow([record["type"], record["props"]])

        with hdfs_client.write(f"{hdfs_dir}/relationships.json", encoding="utf-8") as writer:
            writer.write("[")
            first = True
            rels = session.run("MATCH ()-[r]->() RETURN type(r) AS type, properties(r) AS props")
            for record in rels:
                if not first:
                    writer.write(",")
                writer.write(json.dumps({"type": record["type"], "properties": record["props"]}, default=str))
                first = False
            writer.write("]")

    driver.close()

# =========================
# Utilisation
# =========================

if __name__ == "__main__":
    # extract_postgresql("localhost", "education", "user", "password")
    export_mysql_to_hdfs_csv_json("mysql-api", "dataflow360", "root", "1234")
    export_mongodb_to_hdfs_csv_json("mongodb://root:root@localhost:27017", "datasetMongo")
    export_mongodb_to_hdfs_csv_json("mongodb://root:root@localhost:27017", "education")
    export_cassandra_to_hdfs_csv_json(["cassandra"], "education")
    export_neo4j_to_hdfs_csv_json("bolt://localhost:7687", "neo4j", "12345678")