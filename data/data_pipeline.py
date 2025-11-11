"""
Agricultural Data Engineering Pipeline
Head of Data Engineering Technical Assessment
Author: Emmanuel Nwosu
Date: November 2025

Note on Scalability:
This implementation uses Pandas for the assessment, but the logic maps 1:1
to Spark / Databricks:

- groupBy operations         → Spark DataFrame.groupBy
- window / consecutive logic → Spark window functions
- CSV/Parquet writes         → Delta Lake / table writes

For millions of farms and billions of readings, this pipeline would run on
Spark/Databricks with the same business logic but cluster-based execution.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles common NumPy types."""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class AgricultureDataPipeline:
    """
    A scalable data pipeline for processing agricultural sensor data.

    Demonstrates:
    - data quality handling
    - business-rule-driven transformations
    - simple lineage/metadata tracking
    """

    def __init__(
        self,
        input_path: str = "data/input",
        output_path: str = "data/output",
    ):
        """Initialize the pipeline with input and output paths."""
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

        # Configurable business rules.
        # In production these would typically come from external config
        # (e.g. JSON, environment variables, or a governance/config table).
        self.moisture_threshold = 30.0          # % threshold for low moisture alerts
        self.consecutive_days_threshold = 3     # days required for alert generation
        self.critical_severity_threshold = 20.0 # % threshold for critical alerts

        # Metadata / lineage tracking
        self.pipeline_metadata = {
            "pipeline_name": "Agriculture Data Platform",
            "version": "1.0.0",
            "execution_timestamp": datetime.now().isoformat(),
            "data_sources": [],
            "transformations_applied": [],
            "quality_checks": [],
        }

    # ------------------------
    # Ingestion & data quality
    # ------------------------
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load and register input data sources."""
        logger.info("Loading data files...")

        farms_df = pd.read_csv(self.input_path / "farms.csv")
        self.pipeline_metadata["data_sources"].append(
            {
                "name": "farms.csv",
                "rows_loaded": len(farms_df),
                "columns": list(farms_df.columns),
            }
        )

        sensor_df = pd.read_csv(self.input_path / "sensor_readings.csv")
        self.pipeline_metadata["data_sources"].append(
            {
                "name": "sensor_readings.csv",
                "rows_loaded": len(sensor_df),
                "columns": list(sensor_df.columns),
            }
        )

        logger.info(
            "Loaded %s farms and %s sensor readings",
            len(farms_df),
            len(sensor_df),
        )
        return farms_df, sensor_df

    def clean_and_prepare_data(
        self,
        farms_df: pd.DataFrame,
        sensor_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Clean, standardise, and quality-check the data."""
        logger.info("Cleaning and preparing data...")

        # Track initial counts
        initial_farms = len(farms_df)
        initial_sensors = len(sensor_df)

        # De-duplicate farms and readings
        farms_df = farms_df.drop_duplicates(subset=["farm_id"])
        duplicates_removed_farms = initial_farms - len(farms_df)

        sensor_df = sensor_df.drop_duplicates(subset=["reading_id"])
        duplicates_removed_sensors = initial_sensors - len(sensor_df)

        # Fix incomplete sensor_type values
        sensor_df["sensor_type"] = sensor_df["sensor_type"].replace(
            "SoilM", "SoilMoisture"
        )

        # Ensure reading_value is numeric (critical for quality)
        sensor_df["reading_value"] = pd.to_numeric(
            sensor_df["reading_value"],
            errors="coerce",
        )

        # Standardise timestamp format and fix obvious typos (e.g. 20023 → 2023)
        sensor_df["reading_ts"] = sensor_df["reading_ts"].str.replace(
            "20023", "2023"
        )
        sensor_df["reading_ts"] = sensor_df["reading_ts"].str.strip()

        sensor_df["reading_ts"] = pd.to_datetime(
            sensor_df["reading_ts"],
            format="%d/%m/%Y %H:%M",
            errors="coerce",
        )

        # Standardise farms start_date
        farms_df["start_date"] = pd.to_datetime(farms_df["start_date"])

        # Handle missing values
        null_before = sensor_df.isnull().sum().sum()

        # Remove rows with null readings or timestamps (critical fields)
        sensor_df = sensor_df.dropna(subset=["reading_value", "reading_ts"])

        null_after = sensor_df.isnull().sum().sum()

        quality_checks = {
            "duplicates_removed_farms": duplicates_removed_farms,
            "duplicates_removed_sensors": duplicates_removed_sensors,
            "null_values_removed": null_before - null_after,
            "timestamp_standardization": "Completed",
            "sensor_type_fixes": "SoilM -> SoilMoisture",
        }

        self.pipeline_metadata["quality_checks"].append(quality_checks)
        self.pipeline_metadata["transformations_applied"].append(
            "Data cleaning and standardisation"
        )

        logger.info(
            "Cleaning complete. Removed %s duplicate sensor readings",
            duplicates_removed_sensors,
        )

        return farms_df, sensor_df

    # ------------------------
    # Transformations
    # ------------------------
    def transform_data(
        self,
        farms_df: pd.DataFrame,
        sensor_df: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """
        Transform data to create analytical datasets:

        1. Daily metrics per farm & sensor type
        2. Low soil moisture alerts based on business rules
        """
        logger.info("Transforming data...")

        # Enrich sensor data with farm attributes
        merged_df = sensor_df.merge(farms_df, on="farm_id", how="left")

        # Date column for daily aggregation
        merged_df["date"] = merged_df["reading_ts"].dt.date

        # Daily metrics (mean/min/max/std/count) by farm, date, sensor type & context
        daily_metrics = (
            merged_df.groupby(
                ["farm_id", "date", "sensor_type", "region", "crop_type"]
            )
            .agg({"reading_value": ["mean", "min", "max", "std", "count"]})
            .round(2)
        )

        daily_metrics.columns = [
            "_".join(col).strip() for col in daily_metrics.columns
        ]
        daily_metrics = daily_metrics.reset_index()

        # Split out by sensor type
        soil_moisture_daily = daily_metrics[
            daily_metrics["sensor_type"] == "SoilMoisture"
        ].copy()

        temperature_daily = daily_metrics[
            daily_metrics["sensor_type"] == "Temperature"
        ].copy()

        # Low moisture alerts (parameterised thresholds)
        low_moisture_alerts = self._identify_low_moisture_farms(
            soil_moisture_daily
        )

        self.pipeline_metadata["transformations_applied"].extend(
            [
                "Daily aggregation by farm, sensor type, region, crop_type",
                "Low moisture alert detection",
            ]
        )

        return {
            "daily_soil_moisture": soil_moisture_daily,
            "daily_temperature": temperature_daily,
            "low_moisture_alerts": low_moisture_alerts,
            "merged_data": merged_df,
        }

    def _identify_low_moisture_farms(
        self,
        soil_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Identify farms with consecutive days of low soil moisture."""
        alerts = []

        for farm_id in soil_df["farm_id"].unique():
            farm_data = soil_df[soil_df["farm_id"] == farm_id].sort_values(
                "date"
            )
            if farm_data.empty:
                continue

            # Filter days below moisture threshold
            low_moisture = farm_data[
                farm_data["reading_value_mean"] < self.moisture_threshold
            ].copy()

            if len(low_moisture) >= self.consecutive_days_threshold:
                low_moisture["date"] = pd.to_datetime(low_moisture["date"])
                low_moisture = low_moisture.sort_values("date")

                # Identify consecutive date groups
                low_moisture["date_diff"] = low_moisture["date"].diff().dt.days
                low_moisture["group"] = (
                    low_moisture["date_diff"] != 1
                ).cumsum()

                consecutive_groups = low_moisture.groupby("group").agg(
                    {
                        "date": ["min", "max", "count"],
                        "reading_value_mean": "mean",
                    }
                )

                for _, row in consecutive_groups.iterrows():
                    days = int(row[("date", "count")])
                    if days >= self.consecutive_days_threshold:
                        avg_moisture = float(
                            row[("reading_value_mean", "mean")]
                        )
                        severity = (
                            "CRITICAL"
                            if avg_moisture < self.critical_severity_threshold
                            else "WARNING"
                        )

                        alerts.append(
                            {
                                "farm_id": farm_id,
                                "start_date": row[("date", "min")],
                                "end_date": row[("date", "max")],
                                "consecutive_days": days,
                                "avg_moisture": round(avg_moisture, 2),
                                "region": farm_data["region"].iloc[0],
                                "crop_type": farm_data["crop_type"].iloc[0],
                                "severity": severity,
                            }
                        )

        alerts_df = pd.DataFrame(alerts)
        logger.info("Identified %s low moisture alerts", len(alerts_df))
        return alerts_df

    # ------------------------
    # Visualisation
    # ------------------------
    def create_visualizations(self, transformed_data: Dict[str, pd.DataFrame]) -> None:
        """Create and save a simple analytics dashboard as a PNG."""
        logger.info("Creating visualizations...")

        plt.style.use("seaborn-v0_8-darkgrid")
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        soil_data = transformed_data["daily_soil_moisture"]
        temp_data = transformed_data["daily_temperature"]
        alerts = transformed_data["low_moisture_alerts"]
        merged_data = transformed_data["merged_data"]

        # 1. Soil Moisture Distribution by Region
        if not soil_data.empty:
            ax1 = axes[0, 0]
            soil_by_region = soil_data.groupby("region")[
                "reading_value_mean"
            ].mean()
            soil_by_region.plot(kind="bar", ax=ax1)
            ax1.set_title("Average Soil Moisture by Region", fontsize=12)
            ax1.set_xlabel("Region")
            ax1.set_ylabel("Soil Moisture (%)")
            ax1.axhline(
                y=self.moisture_threshold,
                color="red",
                linestyle="--",
                alpha=0.5,
                label="Low Moisture Threshold",
            )
            ax1.legend()

        # 2. Temperature Trends Over Time
        if not temp_data.empty:
            ax2 = axes[0, 1]
            temp_data["date"] = pd.to_datetime(temp_data["date"])
            daily_avg_temp = temp_data.groupby("date")[
                "reading_value_mean"
            ].mean()
            ax2.plot(daily_avg_temp.index, daily_avg_temp.values, linewidth=2)
            ax2.set_title("Temperature Trends Over Time", fontsize=12)
            ax2.set_xlabel("Date")
            ax2.set_ylabel("Temperature (°C)")
            ax2.tick_params(axis="x", rotation=45)

        # 3. Alert Distribution by Severity
        if not alerts.empty:
            ax3 = axes[1, 0]
            alerts["severity"].value_counts().plot(
                kind="pie",
                ax=ax3,
                autopct="%1.1f%%",
            )
            ax3.set_title(
                "Low Moisture Alert Severity Distribution",
                fontsize=12,
            )
            ax3.set_ylabel("")

        # 4. Sensor Reading Count by Farm
        ax4 = axes[1, 1]
        readings_per_farm = merged_data.groupby("farm_id").size().head(10)
        readings_per_farm.plot(kind="barh", ax=ax4)
        ax4.set_title("Top 10 Farms by Sensor Reading Count", fontsize=12)
        ax4.set_xlabel("Number of Readings")
        ax4.set_ylabel("Farm ID")

        plt.suptitle(
            "Agricultural Data Platform - Analytics Dashboard",
            fontsize=14,
        )
        plt.tight_layout()

        out_path = self.output_path / "analytics_dashboard.png"
        plt.savefig(out_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        logger.info("Visualizations saved to %s", out_path)

    # ------------------------
    # Output & reporting
    # ------------------------
    def save_outputs(self, transformed_data: Dict[str, pd.DataFrame]) -> None:
        """Persist transformed datasets and metadata."""
        logger.info("Saving output files...")

        transformed_data["daily_soil_moisture"].to_csv(
            self.output_path / "daily_soil_moisture.csv",
            index=False,
        )

        transformed_data["daily_temperature"].to_csv(
            self.output_path / "daily_temperature.csv",
            index=False,
        )

        alerts = transformed_data["low_moisture_alerts"]
        if not alerts.empty:
            alerts.to_csv(
                self.output_path / "low_moisture_alerts.csv",
                index=False,
            )

            alerts_json = alerts.copy()
            alerts_json["start_date"] = alerts_json["start_date"].astype(str)
            alerts_json["end_date"] = alerts_json["end_date"].astype(str)

            with open(
                self.output_path / "low_moisture_alerts.json",
                "w",
            ) as f:
                json.dump(
                    alerts_json.to_dict("records"),
                    f,
                    indent=2,
                    cls=NumpyEncoder,
                )

        with open(self.output_path / "pipeline_metadata.json", "w") as f:
            json.dump(
                self.pipeline_metadata,
                f,
                indent=2,
                cls=NumpyEncoder,
            )

        logger.info("All outputs saved successfully")

    def generate_summary_report(
        self,
        transformed_data: Dict[str, pd.DataFrame],
    ) -> None:
        """Generate a human-readable summary report."""
        report = []
        report.append("=" * 60)
        report.append("AGRICULTURAL DATA PLATFORM - PIPELINE EXECUTION SUMMARY")
        report.append("=" * 60)
        report.append(
            f"\nExecution Time: {self.pipeline_metadata['execution_timestamp']}"
        )
        report.append(f"Pipeline Version: {self.pipeline_metadata['version']}")

        report.append("\n## DATA SOURCES")
        for source in self.pipeline_metadata["data_sources"]:
            report.append(
                f"  - {source['name']}: {source['rows_loaded']} rows"
            )

        report.append("\n## DATA QUALITY")
        for check in self.pipeline_metadata["quality_checks"]:
            report.append(
                f"  - Duplicates removed (farms): "
                f"{check['duplicates_removed_farms']}"
            )
            report.append(
                f"  - Duplicates removed (sensors): "
                f"{check['duplicates_removed_sensors']}"
            )
            report.append(
                f"  - Null values removed: {check['null_values_removed']}"
            )

        report.append("\n## KEY INSIGHTS")

        alerts = transformed_data["low_moisture_alerts"]
        if not alerts.empty:
            report.append(
                "  - Farms with low moisture alerts: "
                f"{alerts['farm_id'].nunique()}"
            )
            report.append(f"  - Total alert periods: {len(alerts)}")
            report.append(
                "  - Critical severity alerts: "
                f"{(alerts['severity'] == 'CRITICAL').sum()}"
            )
            report.append(
                "  - Warning severity alerts: "
                f"{(alerts['severity'] == 'WARNING').sum()}"
            )

            if "region" in alerts.columns:
                most_affected = alerts["region"].value_counts().head(1)
                if not most_affected.empty:
                    region = most_affected.index[0]
                    count = most_affected.values[0]
                    report.append(
                        f"  - Most affected region: {region} ({count} alerts)"
                    )

        soil_data = transformed_data["daily_soil_moisture"]
        if not soil_data.empty:
            avg_soil = soil_data["reading_value_mean"].mean()
            report.append(
                f"\n  - Average soil moisture across all farms: {avg_soil:.2f}%"
            )

        temp_data = transformed_data["daily_temperature"]
        if not temp_data.empty:
            avg_temp = temp_data["reading_value_mean"].mean()
            report.append(
                f"  - Average temperature across all farms: {avg_temp:.2f}°C"
            )

        report.append("\n## OUTPUT FILES GENERATED")
        report.extend(
            [
                "  - daily_soil_moisture.csv",
                "  - daily_temperature.csv",
                "  - low_moisture_alerts.csv",
                "  - low_moisture_alerts.json",
                "  - pipeline_metadata.json",
                "  - analytics_dashboard.png",
            ]
        )

        report.append("\n" + "=" * 60)

        report_text = "\n".join(report)
        with open(self.output_path / "pipeline_summary.txt", "w") as f:
            f.write(report_text)

        print(report_text)

    # ------------------------
    # Orchestration
    # ------------------------
    def run(self) -> None:
        """Execute the complete pipeline end-to-end."""
        try:
            logger.info("Starting Agricultural Data Pipeline...")

            farms_df, sensor_df = self.load_data()
            farms_df, sensor_df = self.clean_and_prepare_data(
                farms_df,
                sensor_df,
            )
            transformed_data = self.transform_data(farms_df, sensor_df)
            self.create_visualizations(transformed_data)
            self.save_outputs(transformed_data)
            self.generate_summary_report(transformed_data)

            logger.info("Pipeline execution completed successfully!")
        except Exception as exc:
            logger.error("Pipeline failed: %s", exc)
            raise


if __name__ == "__main__":
    pipeline = AgricultureDataPipeline(
        input_path="data/input",
        output_path="data/output",
    )
    pipeline.run()
