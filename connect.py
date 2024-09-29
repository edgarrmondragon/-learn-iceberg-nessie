import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

# Load the catalog
catalog = load_catalog(name="rest")

# Create a namespace
catalog.create_namespace_if_not_exists("docs_example")

# Confirm the namespace was created
ns = catalog.list_namespaces()
print(ns)

# Load a local parquet file as a PyArrow table
df = pq.read_table("./yellow_tripdata_2023-01.parquet")

# Create a table in the catalog and append data
print("Creating table...")
table = catalog.create_table_if_not_exists(
    "docs_example.taxi_dataset",
    schema=df.schema,
)

# TODO: This is currently failing with
# AWS Error ACCESS_DENIED during CreateMultipartUpload operation: Access Denied.
print("Appending data...")
table.append(df)
print("Rows:", len(table.scan().to_arrow()))

# Now generate a tip-per-mile feature to train the model on
print("Creating tip_per_mile column...")
df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))

# Evolve the schema of the table with the new column
print("Updating schema with new column...")
with table.update_schema() as update_schema:
    update_schema.union_by_name(df.schema)

# Overwrite the table with the new data
table.overwrite(df)
print(table.scan().to_arrow())
print("Rows:", len(table.scan().to_arrow()))

# Filter the table
df = table.scan(row_filter="tip_per_mile > 0").to_arrow()
print("Filtered rows:", len(df))
