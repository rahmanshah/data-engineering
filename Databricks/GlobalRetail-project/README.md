Data was processed using the Medallion Architecture, which includes the following layers:
- **Bronze layer**: Raw data ingestion where data was uploaded to volume as we are using free edition.
- **Silver layer**: Data cleaning, validation, and enrichment to create refined datasets.
- **Gold layer**: Dimensional modeling and aggregation to produce highly refined views for analytics and reporting.

The processed data was then used to generate a report using Power BI Desktop, which was later imported using Microsoft Fabric.