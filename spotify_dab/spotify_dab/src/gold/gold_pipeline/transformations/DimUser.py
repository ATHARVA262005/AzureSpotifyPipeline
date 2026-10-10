import dlt
from pyspark.sql.functions import col

expectations = {
    "rule_1" : "user_id IS NOT NULL"
}

@dlt.table(
    comment="Cleansed staging layer for dimuser"
)
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    return spark.readStream.table("spotify_cata.silver.dimuser")

dlt.create_streaming_table(
    name="dimuser",
    comment="Final Gold SCD Type 2 User Dimension Table"
)

dlt.create_auto_cdc_flow(
    target = "dimuser",
    source = "dimuser_stg",
    keys = ["user_id"],
    sequence_by = col("updated_at"),
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False
)
