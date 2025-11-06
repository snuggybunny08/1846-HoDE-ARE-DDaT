# Agricultural Data Engineering Pipeline

## Head of Data Engineering - Technical Assessment

**Author:** Emmanuel  
**Date:** November 2025  
**Scottish Government - ARE Directorate**

## Overview

This solution demonstrates a production-ready data engineering pipeline for processing agricultural IoT sensor data. The pipeline showcases modern data engineering practices including data quality management, transformation, visualization, and metadata tracking.

## Features

- **Data Ingestion**: Robust CSV data loading with validation
- **Data Quality Management**: 
  - Duplicate detection and removal
  - Null value handling
  - Data type standardization
  - Timestamp normalization
- **Advanced Transformations**:
  - Daily aggregation of sensor metrics
  - Consecutive low moisture alert detection
  - Regional and crop-type analysis
- **Data Lineage & Metadata**: Complete pipeline execution tracking
- **Visualization**: Automated dashboard generation
- **Multiple Output Formats**: CSV, JSON for different use cases

## Installation

### Requirements
- Python 3.8+
- Required packages: pandas, numpy, matplotlib, seaborn

### Setup
```bash
# Clone the repository
git clone [repository-url]
cd agriculture-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Create directory structure
mkdir -p data/input data/output
```

## Usage

### Running the Pipeline

1. Place the input data files in the `data/input/` directory:
   - `farms.csv` - Farm metadata
   - `sensor_readings.csv` - IoT sensor readings

2. Execute the pipeline:
```bash
python data_pipeline.py
```

3. Find outputs in `data/output/` directory:
   - `daily_soil_moisture.csv` - Daily aggregated soil moisture data
   - `daily_temperature.csv` - Daily aggregated temperature data
   - `low_moisture_alerts.csv` - Farms with critical moisture levels
   - `low_moisture_alerts.json` - Alerts in JSON format
   - `analytics_dashboard.png` - Visualization dashboard
   - `pipeline_metadata.json` - Complete execution metadata
   - `pipeline_summary.txt` - Human-readable execution summary

## Pipeline Architecture

### Data Flow
```
Raw Data (CSV) → Ingestion → Quality Checks → Transformation → Analytics → Output
                                ↓                    ↓             ↓
                            Metadata           Aggregation    Visualization
```

### Key Components

1. **Data Ingestion Layer**
   - CSV file reading with schema validation
   - Initial data profiling

2. **Quality Management Layer**
   - Duplicate detection
   - Missing value handling
   - Data type standardization
   - Anomaly detection

3. **Transformation Layer**
   - Daily metric aggregation
   - Alert generation logic
   - Regional analysis

4. **Analytics Layer**
   - Statistical summaries
   - Trend analysis
   - Alert prioritization

5. **Output Layer**
   - Multiple format support
   - Metadata tracking
   - Visualization generation

## Alert Detection Algorithm

The pipeline identifies farms with potential irrigation issues using a sliding window approach:

1. Calculates daily average soil moisture per farm
2. Identifies days with moisture < 30%
3. Detects consecutive low-moisture periods
4. Categorizes severity:
   - **CRITICAL**: Average moisture < 20%
   - **WARNING**: Average moisture 20-30%

## Data Quality Metrics

The pipeline tracks and reports:
- Duplicate records removed
- Null values handled
- Data type conversions
- Timestamp standardization
- Schema validation results

## Metadata & Lineage

Complete data lineage tracking including:
- Source file information
- Transformation steps applied
- Quality check results
- Execution timestamps
- Pipeline version

## Visualization Dashboard

The automated dashboard includes:
1. Average Soil Moisture by Region
2. Temperature Trends Over Time
3. Alert Severity Distribution
4. Top Farms by Sensor Reading Count

## Configuration

Key parameters can be modified in the pipeline class:
- Low moisture threshold: 30% (default)
- Consecutive days for alert: 3 (default)
- Critical severity threshold: 20% (default)

## Error Handling

The pipeline includes robust error handling for:
- Missing files
- Malformed data
- Date parsing errors
- Type conversion issues
- Insufficient data for analysis

## Performance Considerations

- Efficient pandas operations for large datasets
- Memory-optimized data types
- Chunking strategy for very large files (future enhancement)

## Future Enhancements

Potential improvements for production deployment:
1. Apache Spark integration for distributed processing
2. Real-time streaming with Kafka
3. Cloud storage integration (S3/Azure Blob)
4. Automated data quality monitoring
5. Machine learning for anomaly detection
6. RESTful API for pipeline triggers
7. Containerization with Docker
8. Orchestration with Airflow/Dagster

## Testing

Run tests:
```bash
python -m pytest tests/
```

## Support

For questions about this technical assessment:
- Email: Digitalcareers@gov.scot
- Reference: Position 1846 - Head of Data Engineering

## License

This code is part of a technical assessment for the Scottish Government.
