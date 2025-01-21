from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType

schema = Schema(
  NestedField(field_id=1, name='id', field_type=IntegerType(), required=True),
  NestedField(field_id=2, name='name', field_type=StringType(), required=True),
  NestedField(field_id=3, name='value', field_type=IntegerType(), required=True),
)
catalog=load_catalog("local")
catalog.create_namespace_if_not_exists('metrics')
iceberg_table = catalog.create_table_if_not_exists(
  identifier='metrics.data_points',
  schema=schema
)

print(iceberg_table.schema())

