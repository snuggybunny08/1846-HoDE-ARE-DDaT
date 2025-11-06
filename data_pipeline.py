"""
Agricultural Data Engineering Pipeline
Head of Data Engineering Technical Assessment
Author: Emmanuel
Date: November 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple

# Custom JSON encoder for numpy types
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AgricultureDataPipeline:
    """
    A scalable data pipeline for processing agricultural sensor data.
    Demonstrates data quality, transformation, and lineage capabilities.
    """
    
    def __init__(self, input_path: str = "data/input", output_path: str = "data/output"):
        """Initialize the pipeline with input and output paths."""
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Metadata tracking
        self.pipeline_metadata = {
            "pipeline_name": "Agriculture Data Platform",
            "version": "1.0.0",
            "execution_timestamp": datetime.now().isoformat(),
            "data_sources": [],
            "transformations_applied": [],
            "quality_checks": []
        }
        
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load and validate input data files."""
        logger.info("Loading data files...")
        
        # Load farms data
        farms_df = pd.read_csv(self.input_path / "farms.csv")
        self.pipeline_metadata["data_sources"].append({
            "name": "farms.csv",
            "rows_loaded": len(farms_df),
            "columns": list(farms_df.columns)
        })
        
        # Load sensor readings
        sensor_df = pd.read_csv(self.input_path / "sensor_readings.csv")
        self.pipeline_metadata["data_sources"].append({
            "name": "sensor_readings.csv",
            "rows_loaded": len(sensor_df),
            "columns": list(sensor_df.columns)
        })
        
        logger.info(f"Loaded {len(farms_df)} farms and {len(sensor_df)} sensor readings")
        return farms_df, sensor_df
    
    def clean_and_prepare_data(self, farms_df: pd.DataFrame, sensor_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Clean and standardize the data."""
        logger.info("Cleaning and preparing data...")
        
        # Track initial counts
        initial_farms = len(farms_df)
        initial_sensors = len(sensor_df)
        
        # Handle duplicates in farms
        farms_df = farms_df.drop_duplicates(subset=['farm_id'])
        duplicates_removed_farms = initial_farms - len(farms_df)
        
        # Handle duplicates in sensor readings
        sensor_df = sensor_df.drop_duplicates(subset=['reading_id'])
        duplicates_removed_sensors = initial_sensors - len(sensor_df)
        
        # Fix incomplete sensor_type values
        sensor_df['sensor_type'] = sensor_df['sensor_type'].replace('SoilM', 'SoilMoisture')
        
        # Standardize timestamp format
        # Handle different date formats (DD/MM/YYYY HH:MM) and fix typos
        # First fix obvious typos (like 20023 -> 2023)
        sensor_df['reading_ts'] = sensor_df['reading_ts'].str.replace('20023', '2023')
        sensor_df['reading_ts'] = sensor_df['reading_ts'].str.strip()  # Remove extra spaces
        
        # Now parse dates with mixed format handling
        sensor_df['reading_ts'] = pd.to_datetime(sensor_df['reading_ts'], format='%d/%m/%Y %H:%M', errors='coerce')
        
        # Convert start_date for farms
        farms_df['start_date'] = pd.to_datetime(farms_df['start_date'])
        
        # Handle missing values
        null_before = sensor_df.isnull().sum().sum()
        
        # Remove rows with null readings (critical data)
        sensor_df = sensor_df.dropna(subset=['reading_value', 'reading_ts'])
        
        null_after = sensor_df.isnull().sum().sum()
        
        # Data quality checks
        quality_checks = {
            "duplicates_removed_farms": duplicates_removed_farms,
            "duplicates_removed_sensors": duplicates_removed_sensors,
            "null_values_removed": null_before - null_after,
            "timestamp_standardization": "Completed",
            "sensor_type_fixes": "SoilM -> SoilMoisture"
        }
        
        self.pipeline_metadata["quality_checks"].append(quality_checks)
        self.pipeline_metadata["transformations_applied"].append("Data Cleaning and Standardization")
        
        logger.info(f"Cleaning complete. Removed {duplicates_removed_sensors} duplicate readings")
        
        return farms_df, sensor_df
    
    def transform_data(self, farms_df: pd.DataFrame, sensor_df: pd.DataFrame) -> Dict:
        """
        Transform data to create analytical datasets.
        1. Calculate average daily metrics per farm
        2. Identify farms with low soil moisture alerts
        """
        logger.info("Transforming data...")
        
        # Merge farm and sensor data
        merged_df = sensor_df.merge(farms_df, on='farm_id', how='left')
        
        # Add date column for daily aggregation
        merged_df['date'] = merged_df['reading_ts'].dt.date
        
        # 1. Calculate average daily metrics per farm
        daily_metrics = merged_df.groupby(['farm_id', 'date', 'sensor_type', 'region', 'crop_type']).agg({
            'reading_value': ['mean', 'min', 'max', 'std', 'count']
        }).round(2)
        
        daily_metrics.columns = ['_'.join(col).strip() for col in daily_metrics.columns]
        daily_metrics = daily_metrics.reset_index()
        
        # Separate by sensor type for cleaner output
        soil_moisture_daily = daily_metrics[daily_metrics['sensor_type'] == 'SoilMoisture'].copy()
        temperature_daily = daily_metrics[daily_metrics['sensor_type'] == 'Temperature'].copy()
        
        # 2. Identify farms with low soil moisture (<30%) for 3+ consecutive days
        low_moisture_alerts = self._identify_low_moisture_farms(soil_moisture_daily)
        
        self.pipeline_metadata["transformations_applied"].extend([
            "Daily aggregation by farm and sensor type",
            "Low moisture alert detection"
        ])
        
        return {
            "daily_soil_moisture": soil_moisture_daily,
            "daily_temperature": temperature_daily,
            "low_moisture_alerts": low_moisture_alerts,
            "merged_data": merged_df
        }
    
    def _identify_low_moisture_farms(self, soil_df: pd.DataFrame) -> pd.DataFrame:
        """Identify farms with consecutive days of low soil moisture."""
        alerts = []
        
        for farm_id in soil_df['farm_id'].unique():
            farm_data = soil_df[soil_df['farm_id'] == farm_id].sort_values('date')
            
            if len(farm_data) == 0:
                continue
                
            # Check for consecutive days with low moisture
            low_moisture = farm_data[farm_data['reading_value_mean'] < 30].copy()
            
            if len(low_moisture) >= 3:
                # Check for consecutive dates
                low_moisture['date'] = pd.to_datetime(low_moisture['date'])
                low_moisture = low_moisture.sort_values('date')
                
                # Group consecutive dates
                low_moisture['date_diff'] = low_moisture['date'].diff().dt.days
                low_moisture['group'] = (low_moisture['date_diff'] != 1).cumsum()
                
                # Find groups with 3+ consecutive days
                consecutive_groups = low_moisture.groupby('group').agg({
                    'date': ['min', 'max', 'count'],
                    'reading_value_mean': 'mean'
                })
                
                for idx, row in consecutive_groups.iterrows():
                    if row[('date', 'count')] >= 3:
                        alerts.append({
                            'farm_id': farm_id,
                            'start_date': row[('date', 'min')],
                            'end_date': row[('date', 'max')],
                            'consecutive_days': int(row[('date', 'count')]),
                            'avg_moisture': round(row[('reading_value_mean', 'mean')], 2),
                            'region': farm_data['region'].iloc[0],
                            'crop_type': farm_data['crop_type'].iloc[0],
                            'severity': 'CRITICAL' if row[('reading_value_mean', 'mean')] < 20 else 'WARNING'
                        })
        
        alerts_df = pd.DataFrame(alerts)
        logger.info(f"Identified {len(alerts_df)} low moisture alerts")
        
        return alerts_df
    
    def create_visualizations(self, transformed_data: Dict) -> None:
        """Create visualizations for the data."""
        logger.info("Creating visualizations...")
        
        # Set style
        sns.set_style('darkgrid')
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # 1. Soil Moisture Distribution by Region
        soil_data = transformed_data['daily_soil_moisture']
        if not soil_data.empty:
            ax1 = axes[0, 0]
            soil_by_region = soil_data.groupby('region')['reading_value_mean'].mean()
            soil_by_region.plot(kind='bar', ax=ax1, color='skyblue')
            ax1.set_title('Average Soil Moisture by Region', fontsize=12, fontweight='bold')
            ax1.set_xlabel('Region')
            ax1.set_ylabel('Soil Moisture (%)')
            ax1.axhline(y=30, color='red', linestyle='--', alpha=0.5, label='Low Moisture Threshold')
            ax1.legend()
        
        # 2. Temperature Trends Over Time
        temp_data = transformed_data['daily_temperature']
        if not temp_data.empty:
            ax2 = axes[0, 1]
            temp_data['date'] = pd.to_datetime(temp_data['date'])
            daily_avg_temp = temp_data.groupby('date')['reading_value_mean'].mean()
            ax2.plot(daily_avg_temp.index, daily_avg_temp.values, color='orange', linewidth=2)
            ax2.set_title('Temperature Trends Over Time', fontsize=12, fontweight='bold')
            ax2.set_xlabel('Date')
            ax2.set_ylabel('Temperature (°C)')
            ax2.tick_params(axis='x', rotation=45)
        
        # 3. Alert Distribution
        alerts = transformed_data['low_moisture_alerts']
        if not alerts.empty:
            ax3 = axes[1, 0]
            alerts['severity'].value_counts().plot(kind='pie', ax=ax3, autopct='%1.1f%%', 
                                                   colors=['#ff9999', '#ffcc99'])
            ax3.set_title('Low Moisture Alert Severity Distribution', fontsize=12, fontweight='bold')
            ax3.set_ylabel('')
        
        # 4. Sensor Reading Count by Farm
        merged_data = transformed_data['merged_data']
        ax4 = axes[1, 1]
        readings_per_farm = merged_data.groupby('farm_id').size().head(10)
        readings_per_farm.plot(kind='barh', ax=ax4, color='lightgreen')
        ax4.set_title('Top 10 Farms by Sensor Reading Count', fontsize=12, fontweight='bold')
        ax4.set_xlabel('Number of Readings')
        ax4.set_ylabel('Farm ID')
        
        plt.suptitle('Agricultural Data Platform - Analytics Dashboard', fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        # Save the figure
        plt.savefig(self.output_path / 'analytics_dashboard.png', dpi=300, bbox_inches='tight')
        logger.info("Visualizations saved to analytics_dashboard.png")
        
    def save_outputs(self, transformed_data: Dict) -> None:
        """Save transformed data in structured formats."""
        logger.info("Saving output files...")
        
        # Save as CSV files (compatible format)
        transformed_data['daily_soil_moisture'].to_csv(
            self.output_path / 'daily_soil_moisture.csv', 
            index=False
        )
        
        transformed_data['daily_temperature'].to_csv(
            self.output_path / 'daily_temperature.csv',
            index=False
        )
        
        # Save alerts as both CSV and JSON for different use cases
        if not transformed_data['low_moisture_alerts'].empty:
            transformed_data['low_moisture_alerts'].to_csv(
                self.output_path / 'low_moisture_alerts.csv',
                index=False
            )
            
            # Convert dates to strings for JSON serialization
            alerts_json = transformed_data['low_moisture_alerts'].copy()
            alerts_json['start_date'] = alerts_json['start_date'].astype(str)
            alerts_json['end_date'] = alerts_json['end_date'].astype(str)
            
            with open(self.output_path / 'low_moisture_alerts.json', 'w') as f:
                json.dump(alerts_json.to_dict('records'), f, indent=2, cls=NumpyEncoder)
        
        # Save pipeline metadata (data lineage)
        with open(self.output_path / 'pipeline_metadata.json', 'w') as f:
            json.dump(self.pipeline_metadata, f, indent=2, cls=NumpyEncoder)
        
        logger.info("All outputs saved successfully")
    
    def generate_summary_report(self, transformed_data: Dict) -> None:
        """Generate a summary report of the pipeline execution."""
        report = []
        report.append("=" * 60)
        report.append("AGRICULTURAL DATA PLATFORM - PIPELINE EXECUTION SUMMARY")
        report.append("=" * 60)
        report.append(f"\nExecution Time: {self.pipeline_metadata['execution_timestamp']}")
        report.append(f"Pipeline Version: {self.pipeline_metadata['version']}")
        
        report.append("\n## DATA SOURCES")
        for source in self.pipeline_metadata['data_sources']:
            report.append(f"  - {source['name']}: {source['rows_loaded']} rows")
        
        report.append("\n## DATA QUALITY")
        for check in self.pipeline_metadata['quality_checks']:
            report.append(f"  - Duplicates removed (farms): {check['duplicates_removed_farms']}")
            report.append(f"  - Duplicates removed (sensors): {check['duplicates_removed_sensors']}")
            report.append(f"  - Null values removed: {check['null_values_removed']}")
        
        report.append("\n## KEY INSIGHTS")
        
        # Calculate statistics
        alerts = transformed_data['low_moisture_alerts']
        if not alerts.empty:
            report.append(f"  - Farms with low moisture alerts: {alerts['farm_id'].nunique()}")
            report.append(f"  - Total alert periods: {len(alerts)}")
            report.append(f"  - Critical severity alerts: {(alerts['severity'] == 'CRITICAL').sum()}")
            report.append(f"  - Warning severity alerts: {(alerts['severity'] == 'WARNING').sum()}")
            
            # Most affected region
            if 'region' in alerts.columns:
                most_affected = alerts['region'].value_counts().head(1)
                if not most_affected.empty:
                    report.append(f"  - Most affected region: {most_affected.index[0]} ({most_affected.values[0]} alerts)")
        
        soil_data = transformed_data['daily_soil_moisture']
        if not soil_data.empty:
            report.append(f"\n  - Average soil moisture across all farms: {soil_data['reading_value_mean'].mean():.2f}%")
        
        temp_data = transformed_data['daily_temperature']
        if not temp_data.empty:
            report.append(f"  - Average temperature across all farms: {temp_data['reading_value_mean'].mean():.2f}°C")
        
        report.append("\n## OUTPUT FILES GENERATED")
        report.append("  - daily_soil_moisture.csv")
        report.append("  - daily_temperature.csv")
        report.append("  - low_moisture_alerts.csv")
        report.append("  - low_moisture_alerts.json")
        report.append("  - pipeline_metadata.json")
        report.append("  - analytics_dashboard.png")
        
        report.append("\n" + "=" * 60)
        
        # Save report
        report_text = "\n".join(report)
        with open(self.output_path / 'pipeline_summary.txt', 'w') as f:
            f.write(report_text)
        
        print(report_text)
        
    def run(self) -> None:
        """Execute the complete pipeline."""
        try:
            logger.info("Starting Agricultural Data Pipeline...")
            
            # Load data
            farms_df, sensor_df = self.load_data()
            
            # Clean and prepare
            farms_df, sensor_df = self.clean_and_prepare_data(farms_df, sensor_df)
            
            # Transform
            transformed_data = self.transform_data(farms_df, sensor_df)
            
            # Create visualizations
            self.create_visualizations(transformed_data)
            
            # Save outputs
            self.save_outputs(transformed_data)
            
            # Generate summary report
            self.generate_summary_report(transformed_data)
            
            logger.info("Pipeline execution completed successfully!")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    # Initialize and run pipeline
    pipeline = AgricultureDataPipeline(
        input_path="data/input",
        output_path="data/output"
    )
    pipeline.run()
