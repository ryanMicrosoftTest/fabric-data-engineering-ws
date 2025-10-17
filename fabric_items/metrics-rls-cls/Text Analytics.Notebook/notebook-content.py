# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5090acaa-9246-4eb2-b9fc-93b8b6e7e60c",
# META       "default_lakehouse_name": "deid_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "5090acaa-9246-4eb2-b9fc-93b8b6e7e60c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Text Analytics Notebook
# - SynapseML
# - Sentiment Analysis
# - Language Detector
# - Key Phrase Extractor
# - Named Entity Recognition
# - Entity Linking

# CELL ********************

import synapse.ml.core
from synapse.ml.cognitive.language import AnalyzeText
from pyspark.sql.functions import col, udf, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

doctor_df = spark.read.load('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/deidentified_dr_notes')

display(doctor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame([
    ("Great atmosphere. Close to plenty of restaurants, hotels, and transit! Staff are friendly and helpful.",),
    ("What a sad story!",)
], ["text"])

model = (AnalyzeText()
        .setTextCol("text")
        .setKind("SentimentAnalysis")
        .setOutputCol("response"))

result = model.transform(df)\
        .withColumn("documents", col("response.documents"))\
        .withColumn("sentiment", col("documents.sentiment"))

display(result.select("text", "sentiment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sentiment analysis
sentiment_model = (
    AnalyzeText().setTextCol('memo_content').setKind('SentimentAnalysis').setOutputCol('sentiment_response')
)

result = (sentiment_model.transform(doctor_df)
               .withColumn('documents', col('sentiment_response.documents'))
               .withColumn('sentiment', col('documents.sentiment'))
)

display(result.select("memo_content", "sentiment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

doctor_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

language_model = (AnalyzeText()
                  .setTextCol('memo_content')
                  .setKind('LanguageDetection')
                  .setOutputCol('response')
)

language_model_result = language_model.transform(doctor_df).withColumn('documents', col('response.documents')).withColumn('detectedLanguage', col('documents.detectedLanguage.name'))

display(
     language_model_result.select('memo_content','detectedLanguage', 'response')   
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# key phrase extractor

key_phrase_extractor_model = (AnalyzeText()
                             .setTextCol('memo_content')
                             .setKind('KeyPhraseExtraction')
                             .setOutputCol('response')
)

key_phrase_result = key_phrase_extractor_model.transform(doctor_df).withColumn('keyPhrases', col('response.documents.keyPhrases'))

display(key_phrase_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 


# CELL ********************

# named entity recognition

ner_model = (AnalyzeText()
            .setTextCol('memo_content')
            .setKind('EntityRecognition')
            .setOutputCol('response')
)

smaller_df = doctor_df.limit(4)

ner_result = (ner_model.transform(smaller_df)
                       .withColumn('documents', col('response.documents'))
                       .withColumn('entities', col('documents.entities'))
)

display(ner_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# entity linking

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
