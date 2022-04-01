import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def get_ccn_alert_from_sfmc_alert(col_name, df, ccn_alert_col, sfmc_alert_col):
    df_collect = df.collect()
    ignore_cols = ['MOBILE_VERIFICATION_STATUS', 'MOBILE_NUMBER', 'EMAIL', 'MOBILE_VERIFICATION_TIMESTAMP']
    
    output = [
        (
            col_name,
            "String",
            col_name.replace(
                str(x[sfmc_alert_col].strip().replace("EMAIL", "").replace("SMS", "")),
                str(x[ccn_alert_col].strip() + "_"),
            ),
            "String",
        )
        for x in df_collect
        if str(x[sfmc_alert_col].strip()) in col_name
    ]
    
    if col_name.strip() == "CIF":
        return (col_name, "String", "GUID_CIF", "String")
        
    if col_name.strip() in ignore_cols:
        return (col_name, "String", col_name, "String")

    return output[0]


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "alert_code_mapping_path",
        "sfmc_to_ccn_pref_path",
        "output_path",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
(
    alert_code_mapping_path,
    sfmc_to_ccn_pref_path,
    output_path,
) = (
    args["alert_code_mapping_path"],
    args["sfmc_to_ccn_pref_path"],
    args["output_path"],
)
ccn_alert_code_col_name = "CCN Alert Code"
sfmc_alert_code_col_name = "SFMC File Alert Code"

alert_code_mapping = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [alert_code_mapping_path]},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="alert_code_mapping",
)
alert_code_mapping_df = alert_code_mapping.toDF()

sfmc_to_ccn_pref = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [sfmc_to_ccn_pref_path]},
    format="csv",
    format_options={"withHeader": True, "separator": "|"},
    transformation_ctx="sfmc_to_ccn_pref",
)
sfmc_to_ccn_pref_df = sfmc_to_ccn_pref.toDF()

print("sfmc-to-ccn-alert-preference BEFORE schema...")
sfmc_to_ccn_pref_df.printSchema()

transformed_mappings = list(
    filter(
        None,
        [
            get_ccn_alert_from_sfmc_alert(
                x,
                alert_code_mapping_df,
                ccn_alert_code_col_name,
                sfmc_alert_code_col_name,
            )
            for x in sfmc_to_ccn_pref_df.columns
        ],
    )
)

sfmc_to_ccn_pref_transformed = ApplyMapping.apply(
    frame=sfmc_to_ccn_pref,
    mappings=transformed_mappings,
    transformation_ctx="sfmc_to_ccn_pref_transformed",
)
sfmc_to_ccn_pref_transformed_df = sfmc_to_ccn_pref_transformed.toDF()
print("sfmc-to-ccn-alert-preference AFTER schema...")
sfmc_to_ccn_pref_transformed_df.printSchema()

print("saving to " + output_path)

sfmc_to_ccn = sfmc_to_ccn_pref_transformed_df.repartition(1)
sfmc_to_ccn.write.parquet(output_path)

job.commit()
