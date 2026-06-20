import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


def normalize_s3_prefix(value):
    value = value.strip()
    if value.lower().startswith("value:"):
        value = value.split(":", 1)[1].strip()
    return value.rstrip("/") + "/"


def optional_arg(name, default):
    flag = "--" + name
    if flag in sys.argv:
        index = sys.argv.index(flag)
        if index + 1 < len(sys.argv):
            return sys.argv[index + 1]
    return default


def read_csv(spark, path):
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(path)
    )


def with_surrogate_key(df, key_name, order_cols):
    window = Window.orderBy(*[F.col(col).asc_nulls_last() for col in order_cols])
    return df.dropDuplicates(order_cols).withColumn(key_name, F.row_number().over(window))


def date_key(col_name):
    return F.date_format(F.to_timestamp(F.col(col_name)), "yyyyMMdd").cast("int")


def time_key(col_name):
    return F.date_format(F.to_timestamp(F.col(col_name)), "HHmmss").cast("int")


def write_table(df, output_s3, table_name, output_format):
    writer = df.write.mode("overwrite")
    path = output_s3 + table_name + "/"
    if output_format == "csv":
        writer.option("header", True).option("compression", "gzip").csv(path)
    elif output_format == "json":
        writer.option("compression", "gzip").json(path)
    else:
        writer.parquet(path)


def timestamp_values(df, *columns):
    selected = None
    for column in columns:
        part = df.select(F.to_timestamp(F.col(column)).alias("ts")).where(F.col("ts").isNotNull())
        selected = part if selected is None else selected.unionByName(part)
    return selected


def apply_scd1(spark, new_df, output_s3, table_name, sk_col, natural_keys, output_format):
    """
    SCD Type 1:
    - existing records matched by natural_keys → overwrite non-key columns, keep old SK
    - existing records not in new data → keep as-is
    - new records not in existing → assign new SK continuing from max existing SK
    """
    path = output_s3 + table_name + "/"

    try:
        if output_format == "csv":
            existing_df = spark.read.option("header", True).csv(path)
        elif output_format == "json":
            existing_df = spark.read.json(path)
        else:
            existing_df = spark.read.parquet(path)

        existing_df = existing_df.withColumn(sk_col, F.col(sk_col).cast("int"))

        # records في الـ new data موجودة في الـ existing → ناخد الـ SK القديم + الـ columns الجديدة
        updated = (
            existing_df.select(sk_col, *natural_keys)
            .join(new_df.drop(sk_col), natural_keys, "inner")
        )

        # records قديمة مش موجودة في الـ new data → نحتفظ بيها زي ما هي
        kept_old = existing_df.join(new_df.select(natural_keys), natural_keys, "left_anti")

        # records جديدة مش موجودة في الـ existing → نديها SK جديد يكمل من بعد الـ max
        brand_new = new_df.drop(sk_col).join(existing_df.select(natural_keys), natural_keys, "left_anti")
        max_sk = existing_df.agg(F.max(F.col(sk_col))).collect()[0][0] or 0
        sk_window = Window.orderBy(*[F.col(c).asc_nulls_last() for c in natural_keys])
        brand_new = brand_new.withColumn(sk_col, (F.row_number().over(sk_window) + F.lit(max_sk)).cast("int"))

        merged = kept_old.unionByName(updated).unionByName(brand_new)

    except Exception:
        # الجدول مش موجود → أول run، نكتب الـ new data عادي
        merged = new_df

    write_table(merged, output_s3, table_name, output_format)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3", "OUTPUT_S3"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw_s3 = normalize_s3_prefix(args["RAW_S3"])
output_s3 = normalize_s3_prefix(args["OUTPUT_S3"])
output_format = optional_arg("OUTPUT_FORMAT", "json").lower()

patients = read_csv(spark, raw_s3 + "PATIENTS.csv")
admissions = read_csv(spark, raw_s3 + "ADMISSIONS.csv")
icustays = read_csv(spark, raw_s3 + "ICUSTAYS.csv")
prescriptions = read_csv(spark, raw_s3 + "PRESCRIPTIONS.csv")
d_items_raw = read_csv(spark, raw_s3 + "D_ITEMS.csv")
diagnoses_icd = read_csv(spark, raw_s3 + "DIAGNOSES_ICD.csv")
d_icd_diagnoses = read_csv(spark, raw_s3 + "D_ICD_DIAGNOSES.csv")
procedures_icd = read_csv(spark, raw_s3 + "PROCEDURES_ICD.csv")
d_icd_procedures = read_csv(spark, raw_s3 + "D_ICD_PROCEDURES.csv")
chartevents = read_csv(spark, raw_s3 + "CHARTEVENTS.csv")
procedureevents_mv = read_csv(spark, raw_s3 + "PROCEDUREEVENTS_MV.csv")
inputevents_cv = read_csv(spark, raw_s3 + "INPUTEVENTS_CV.csv")
inputevents_mv = read_csv(spark, raw_s3 + "INPUTEVENTS_MV.csv")
outputevents = read_csv(spark, raw_s3 + "OUTPUTEVENTS.csv")

# dim_patient → FHIR: Patient
dim_patient = with_surrogate_key(
    patients.select(
        F.col("subject_id").alias("patient_id"),
        "gender",
        date_key("dob").alias("birthDate"),
        date_key("dod").alias("deceasedDatetime"),
        date_key("dod_hosp").alias("deceasedhospDatetime"),
        date_key("dod_ssn").alias("deceasedssnDatetime"),
        F.col("expire_flag").alias("deceasedBoolean"),
    ),
    "patient_sk",
    ["patient_id"],
).select(
    "patient_sk",
    "patient_id",
    "gender",
    "birthDate",
    "deceasedDatetime",
    "deceasedhospDatetime",
    "deceasedssnDatetime",
    "deceasedBoolean",
)

# dim_admission → FHIR: Encounter / Patient / Coverage / Condition
dim_admission = with_surrogate_key(
    admissions.select(
        F.col("hadm_id").alias("encounter_identifier"),
        F.col("admission_type").alias("encounter_class"),
        F.col("admission_location").alias("admit_source"),
        F.col("discharge_location").alias("discharge_disposition"),
        F.col("marital_status").alias("patient_marital_status"),
        F.col("religion").alias("patient_religion_extension"),
        F.col("ethnicity").alias("patient_ethnicity_extension"),
        F.col("insurance").alias("coverage"),
        F.col("language").alias("patient_communication_language"),
        F.col("diagnosis").alias("condition"),
    ),
    "adm_dim_sk",
    ["encounter_identifier"],
).select(
    "adm_dim_sk",
    "encounter_identifier",
    "encounter_class",
    "admit_source",
    "discharge_disposition",
    "patient_marital_status",
    "patient_religion_extension",
    "patient_ethnicity_extension",
    "coverage",
    "patient_communication_language",
    "condition",
)

# dim_icustays → FHIR: Encounter / Location
dim_icustays = with_surrogate_key(
    icustays.select(
        F.col("icustay_id").alias("encounter_id"),
        F.col("dbsource").alias("encounter_extension"),
        F.col("first_careunit").alias("first_care_location"),
        F.col("last_careunit").alias("last_care_location"),
        F.col("first_wardid").alias("first_ward_location"),
        F.col("last_wardid").alias("last_ward_location"),
    ),
    "icu_stay_sk",
    ["encounter_id"],
).select(
    "icu_stay_sk",
    "encounter_id",
    "encounter_extension",
    "first_care_location",
    "last_care_location",
    "first_ward_location",
    "last_ward_location",
)

# dim_drug → FHIR: Medication / MedicationRequest / MedicationDispense
dim_drug = with_surrogate_key(
    prescriptions.select(
        F.col("formulary_drug_cd").alias("medication_code"),
        F.col("ndc").alias("medication_ndc_code"),
        F.col("drug").alias("medication_code_text"),
        F.col("drug_type").alias("medication_type"),
        F.col("prod_strength").alias("ingredient_strength"),
        F.col("route").alias("dosage_instruction_route"),
        F.col("dose_unit_rx").alias("dose_and_rate"),
        F.col("form_unit_disp").alias("medication_form"),
    ),
    "drug_sk",
    ["medication_code", "medication_ndc_code", "medication_code_text", "medication_type", "ingredient_strength", "dosage_instruction_route"],
).select(
    "drug_sk",
    "medication_code",
    "medication_ndc_code",
    "medication_code_text",
    "medication_type",
    "ingredient_strength",
    "dosage_instruction_route",
    "dose_and_rate",
    "medication_form",
)

# dim_items → FHIR: Observation
dim_items = with_surrogate_key(
    d_items_raw.select(
        F.col("itemid").alias("observation_code"),
        F.col("label").alias("observation_code_text"),
        F.col("abbreviation").alias("observation_code_abbr"),
        F.col("dbsource").alias("observation_extension"),
        F.col("linksto").alias("observation_based_on"),
        F.col("category").alias("observation_category"),
        F.col("unitname").alias("value_quantity_unit"),
        F.col("param_type").alias("observation_value_type"),
        F.col("conceptid").alias("observation_concept_code"),
    ),
    "item_sk",
    ["observation_code"],
).select(
    "item_sk",
    "observation_code",
    "observation_code_text",
    "observation_code_abbr",
    "observation_extension",
    "observation_based_on",
    "observation_category",
    "value_quantity_unit",
    "observation_value_type",
    "observation_concept_code",
)

# dim_diagnosis → FHIR: Condition
dim_diagnosis = with_surrogate_key(
    diagnoses_icd.select("icd9_code", "seq_num")
    .join(d_icd_diagnoses.select("icd9_code", "short_title", "long_title"), "icd9_code", "left"),
    "diagnosis_sk",
    ["icd9_code", "seq_num"],
).select(
    "diagnosis_sk",
    F.col("icd9_code").alias("condition_code"),
    F.col("short_title").alias("condition_short_text"),
    F.col("long_title").alias("condition_long_text"),
    F.col("seq_num").alias("condition_extension"),
)

# dim_procedure → FHIR: Procedure
dim_procedure = with_surrogate_key(
    procedures_icd.select("icd9_code", "seq_num")
    .join(d_icd_procedures.select("icd9_code", "short_title", "long_title"), "icd9_code", "left"),
    "procedure_sk",
    ["icd9_code", "seq_num"],
).select(
    "procedure_sk",
    F.col("icd9_code").alias("procedure_code"),
    F.col("short_title").alias("procedure_short_text"),
    F.col("long_title").alias("procedure_long_text"),
    F.col("seq_num").alias("procedure_extension"),
)

all_timestamps = (
    timestamp_values(patients, "dob", "dod", "dod_hosp", "dod_ssn")
    .unionByName(timestamp_values(admissions, "admittime", "dischtime", "deathtime", "edregtime", "edouttime"))
    .unionByName(timestamp_values(icustays, "intime", "outtime"))
    .unionByName(timestamp_values(prescriptions, "startdate", "enddate"))
    .unionByName(timestamp_values(chartevents, "charttime", "storetime"))
    .unionByName(timestamp_values(procedureevents_mv, "starttime", "endtime", "storetime"))
    .unionByName(timestamp_values(inputevents_cv, "charttime", "storetime"))
    .unionByName(timestamp_values(inputevents_mv, "starttime", "endtime", "storetime"))
    .unionByName(timestamp_values(outputevents, "charttime", "storetime"))
)

dim_date = all_timestamps.select(F.to_date("ts").alias("date")).where(F.col("date").isNotNull()).dropDuplicates(
    ["date"]
).select(
    F.date_format("date", "yyyyMMdd").cast("int").alias("date_sk"),
    "date",
    F.year("date").alias("year"),
    F.quarter("date").alias("quarter"),
    F.month("date").alias("month"),
    F.weekofyear("date").alias("week"),
    F.dayofmonth("date").alias("day"),
)

dim_time = all_timestamps.select(
    F.date_format("ts", "HHmmss").cast("int").alias("time_sk"),
    F.date_format("ts", "HH:mm:ss").alias("time"),
    F.hour("ts").alias("hour"),
    F.minute("ts").alias("minute"),
    F.second("ts").alias("second"),
).dropDuplicates(["time_sk"])

patient_bridge = dim_patient.select("patient_sk", "patient_id")
admission_bridge = dim_admission.select("adm_dim_sk", "encounter_identifier")
icu_bridge = dim_icustays.select("icu_stay_sk", "encounter_id")
item_bridge = dim_items.select("item_sk", "observation_code")
drug_bridge = dim_drug.select("drug_sk", "medication_code", "medication_ndc_code", "medication_code_text", "medication_type", "ingredient_strength", "dosage_instruction_route")
diagnosis_bridge = dim_diagnosis.select("diagnosis_sk", "condition_code", "condition_extension")
procedure_bridge = dim_procedure.select("procedure_sk", "procedure_code", "procedure_extension")

# fact_admission → FHIR: Encounter / Patient
fact_admission = (
    admissions.join(patient_bridge, admissions.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, admissions.hadm_id == admission_bridge.encounter_identifier, "left")
    .select(
        F.col("row_id").alias("admission_fact_sk"),
        "patient_sk",
        "adm_dim_sk",
        date_key("admittime").alias("encounter_period_start_sk"),
        date_key("dischtime").alias("encounter_period_end_sk"),
        date_key("deathtime").alias("deceased_date_time_sk"),
        F.lit(None).cast("string").alias("encounter_service_type"),
        F.lit(None).cast("string").alias("prev_encounter_service_type"),
        F.lit(None).cast("int").alias("encounter_location_period_sk"),
        "hospital_expire_flag",
    )
)

# fact_icustays → FHIR: Encounter
fact_icustays = (
    icustays.join(patient_bridge, icustays.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, icustays.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(icu_bridge, icustays.icustay_id == icu_bridge.encounter_id, "left")
    .select(
        F.col("row_id").alias("icu_fact_sk"),
        "patient_sk",
        "adm_dim_sk",
        "icu_stay_sk",
        date_key("intime").alias("encounter_period_start_sk"),
        date_key("outtime").alias("encounter_period_end_sk"),
        F.col("los").alias("encounter_length"),
    )
)

# fact_prescriptions → FHIR: MedicationRequest / MedicationDispense
fact_prescriptions = (
    prescriptions.join(patient_bridge, prescriptions.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, prescriptions.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(icu_bridge, prescriptions.icustay_id == icu_bridge.encounter_id, "left")
    .join(
        drug_bridge,
        (prescriptions.formulary_drug_cd == drug_bridge.medication_code)
        & (prescriptions.ndc == drug_bridge.medication_ndc_code)
        & (prescriptions.drug == drug_bridge.medication_code_text)
        & (prescriptions.drug_type == drug_bridge.medication_type)
        & (prescriptions.prod_strength == drug_bridge.ingredient_strength)
        & (prescriptions.route == drug_bridge.dosage_instruction_route),
        "left",
    )
    .select(
        F.col("row_id").alias("prescription_sk"),
        "patient_sk",
        "adm_dim_sk",
        "icu_stay_sk",
        "drug_sk",
        date_key("startdate").alias("authored_on_sk"),
        date_key("enddate").alias("when_handed_over_sk"),
        F.col("dose_val_rx").alias("dosage_instruction_dose"),
        F.col("form_val_disp").alias("dispense_quantity"),
    )
)

# fact_diag_procedure → FHIR: Condition / Procedure
diagnosis_fact = (
    diagnoses_icd.join(patient_bridge, diagnoses_icd.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, diagnoses_icd.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(
        diagnosis_bridge,
        (diagnoses_icd.icd9_code == diagnosis_bridge.condition_code) & (diagnoses_icd.seq_num == diagnosis_bridge.condition_extension),
        "left",
    )
    .select(
        F.concat(F.lit("D"), F.col("row_id")).alias("diag_proc_sk"),
        "patient_sk",
        "adm_dim_sk",
        "diagnosis_sk",
        F.lit(None).cast("int").alias("procedure_sk"),
    )
)

procedure_fact = (
    procedures_icd.join(patient_bridge, procedures_icd.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, procedures_icd.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(
        procedure_bridge,
        (procedures_icd.icd9_code == procedure_bridge.procedure_code) & (procedures_icd.seq_num == procedure_bridge.procedure_extension),
        "left",
    )
    .select(
        F.concat(F.lit("P"), F.col("row_id")).alias("diag_proc_sk"),
        "patient_sk",
        "adm_dim_sk",
        F.lit(None).cast("int").alias("diagnosis_sk"),
        "procedure_sk",
    )
)

fact_diag_procedure = diagnosis_fact.unionByName(procedure_fact)

# fact_chartevents → FHIR: Observation
fact_chartevents = (
    chartevents.join(patient_bridge, chartevents.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, chartevents.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(icu_bridge, chartevents.icustay_id == icu_bridge.encounter_id, "left")
    .join(item_bridge, chartevents.itemid == item_bridge.observation_code, "left")
    .select(
        F.col("row_id").alias("chart_sk"),
        "patient_sk",
        "adm_dim_sk",
        "icu_stay_sk",
        "item_sk",
        date_key("charttime").alias("effective_date_time_sk"),
        date_key("storetime").alias("issued_sk"),
        F.col("value").alias("value_string"),
        F.col("valuenum").alias("value_quantity_value"),
        F.col("valueuom").alias("value_quantity_unit"),
        F.col("warning").alias("interpretation"),
        F.col("error").alias("note"),
        F.col("resultstatus").alias("status"),
        F.col("stopped").alias("stopped_status"),
    )
)

# fact_procevents → FHIR: Procedure
fact_procevents = (
    procedureevents_mv.join(patient_bridge, procedureevents_mv.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, procedureevents_mv.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(icu_bridge, procedureevents_mv.icustay_id == icu_bridge.encounter_id, "left")
    .join(item_bridge, procedureevents_mv.itemid == item_bridge.observation_code, "left")
    .select(
        F.col("row_id").alias("proc_event_sk"),
        "patient_sk",
        "adm_dim_sk",
        "icu_stay_sk",
        "item_sk",
        date_key("starttime").alias("performed_period_start_sk"),
        date_key("endtime").alias("performed_period_end_sk"),
        F.col("value").alias("procedure_note"),
        F.col("valueuom").alias("procedure_extension"),
    )
)

input_cv_normalized = inputevents_cv.select(
    "row_id",
    "subject_id",
    "hadm_id",
    "icustay_id",
    F.col("charttime").alias("starttime"),
    F.col("charttime").alias("endtime"),
    "storetime",
    "itemid",
    "amount",
    "amountuom",
    "rate",
    "rateuom",
    F.lit(None).cast("string").alias("originalrate"),
    "originalamount",
)

input_mv_normalized = inputevents_mv.select(
    "row_id",
    "subject_id",
    "hadm_id",
    "icustay_id",
    "starttime",
    "endtime",
    "storetime",
    "itemid",
    "amount",
    "amountuom",
    "rate",
    "rateuom",
    "originalrate",
    "originalamount",
)

inputevents = input_cv_normalized.unionByName(input_mv_normalized)

# fact_inputevents → FHIR: MedicationAdministration
fact_inputevents = (
    inputevents.join(patient_bridge, inputevents.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, inputevents.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(icu_bridge, inputevents.icustay_id == icu_bridge.encounter_id, "left")
    .join(item_bridge, inputevents.itemid == item_bridge.observation_code, "left")
    .select(
        F.col("row_id").alias("input_sk"),
        "patient_sk",
        "adm_dim_sk",
        "icu_stay_sk",
        "item_sk",
        date_key("starttime").alias("effective_period_start_sk"),
        date_key("endtime").alias("effective_period_end_sk"),
        date_key("storetime").alias("recorded_sk"),
        F.col("amount").alias("dosage_dose"),
        F.col("amountuom").alias("dosage_dose_unit"),
        F.col("rate").alias("dosage_rate"),
        F.col("rateuom").alias("dosage_rate_unit"),
        F.col("originalrate").alias("original_rate_extension"),
        F.col("originalamount").alias("original_amount_extension"),
    )
)

# fact_outputevents → FHIR: Observation
fact_outputevents = (
    outputevents.join(patient_bridge, outputevents.subject_id == patient_bridge.patient_id, "left")
    .join(admission_bridge, outputevents.hadm_id == admission_bridge.encounter_identifier, "left")
    .join(icu_bridge, outputevents.icustay_id == icu_bridge.encounter_id, "left")
    .join(item_bridge, outputevents.itemid == item_bridge.observation_code, "left")
    .select(
        F.col("row_id").alias("output_sk"),
        "patient_sk",
        "adm_dim_sk",
        "icu_stay_sk",
        "item_sk",
        date_key("charttime").alias("effective_date_time_sk"),
        date_key("storetime").alias("issued_sk"),
        F.col("value").alias("value_quantity_value"),
        F.col("valueuom").alias("value_quantity_unit"),
        F.col("stopped").alias("status"),
        F.col("newbottle").alias("observation_extension"),
        F.col("iserror").alias("observation_note"),
    )
)

dim_scd1 = [
    (dim_patient,   "dim_patient",   "patient_sk",   ["patient_id"]),
    (dim_admission, "dim_admission", "adm_dim_sk",   ["encounter_identifier"]),
    (dim_icustays,  "dim_icustays",  "icu_stay_sk",  ["encounter_id"]),
    (dim_drug,      "dim_drug",      "drug_sk",      ["medication_code", "medication_ndc_code", "medication_code_text",
                                                       "medication_type", "ingredient_strength", "dosage_instruction_route"]),
    (dim_items,     "dim_items",     "item_sk",      ["observation_code"]),
    (dim_diagnosis, "dim_diagnosis", "diagnosis_sk", ["condition_code", "condition_extension"]),
    (dim_procedure, "dim_procedure", "procedure_sk", ["procedure_code", "procedure_extension"]),
]

for df, table_name, sk_col, natural_keys in dim_scd1:
    apply_scd1(spark, df, output_s3, table_name, sk_col, natural_keys, output_format)


fact_tables = {
    "dim_date":            dim_date,
    "dim_time":            dim_time,
    "fact_admission":      fact_admission,
    "fact_icustays":       fact_icustays,
    "fact_prescriptions":  fact_prescriptions,
    "fact_diag_procedure": fact_diag_procedure,
    "fact_chartevents":    fact_chartevents,
    "fact_procevents":     fact_procevents,
    "fact_inputevents":    fact_inputevents,
    "fact_outputevents":   fact_outputevents,
}

for table_name, table_df in fact_tables.items():
    write_table(table_df, output_s3, table_name, output_format)

job.commit()
