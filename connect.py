import contextlib

import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.catalog import load_catalog

# Load the catalog
catalog = load_catalog(name="rest")

# Create a namespace
with contextlib.suppress(NamespaceAlreadyExistsError):
    catalog.create_namespace("docs_example")

# Confirm the namespace was created
ns = catalog.list_namespaces()
print(ns)

# Load a local parquet file as a PyArrow table
df = pq.read_table("./yellow_tripdata_2023-01.parquet")

# Drop the table if it exists
with contextlib.suppress(NoSuchTableError):
    catalog.drop_table("docs_example.taxi_dataset")

# Create a table in the catalog and append data
print("Creating table...")
table = catalog.create_table(
    "docs_example.taxi_dataset",
    schema=df.schema,
)
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
print("Rows:", len(table.scan().to_arrow()))

# Filter the table
df = table.scan(row_filter="tip_per_mile > 0").to_arrow()
print("Filtered rows:", len(df))
