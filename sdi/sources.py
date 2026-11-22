"""Registry of the 7 source domains: file naming, primary keys, staging table names and validation rules.

Validation rules are declarative so the batch loader and the streaming API apply exactly the same checks."""
import re
from dataclasses import dataclass, field
from .source_columns import SOURCE_COLUMNS


def snake(name: str) -> str:
    """CamelCase source header -> snake_case staging column (MedicineID -> medicine_id, GTIN14 -> gtin14)."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z])([A-Z0-9])", r"\1_\2", s)
    s = re.sub(r"([0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


@dataclass
class Rule:
    code: str            # reason code written to rejects.rejected_rows
    kind: str            # not_null | positive | non_negative | range | fk_medicine | equation
    columns: tuple
    params: dict = field(default_factory=dict)


@dataclass
class Source:
    key: str                 # e.g. "05_pharmacy_sales_data"
    domain: str              # short business domain name used on dashboards
    table: str               # staging table name (schema staging)
    pk: tuple                # source primary key columns (CamelCase)
    load_order: int
    ingest_mode: str         # batch | stream (for refresh increments)
    sdi_mart: bool           # used by the Phase-1 SDI mart?
    rules: list = field(default_factory=list)

    @property
    def columns(self):
        return SOURCE_COLUMNS[self.key]

    @property
    def date_columns(self):
        return [c for c, t in self.columns if t == "date"]

    @property
    def coverage_date(self):
        """Business date column used for the storage 'date coverage' metric."""
        return {
            "01_master_data": "EffectiveFromDate", "02_manufacturing_data": "OrderDate",
            "03_manufacturer_delivery_data": "ActualDeliveryDate", "04_warehouse_data": "SnapshotDate",
            "05_pharmacy_sales_data": "SalesDate", "06_hospital_demand_data": "DemandMonth",
            "07_iot_streaming_data": "EventTimestampUTC",
        }[self.key]


DATE_RANGE = {"min": "2020-01-01", "max": "2030-12-31"}

SOURCES = {
    "01_master_data": Source(
        "01_master_data", "Medicine master", "stg_master_data", ("MedicineID", "EffectiveFromDate"), 1, "batch", True,
        [Rule("NULL_KEY", "not_null", ("MedicineID", "GTIN14")),
         Rule("NON_POSITIVE_PRICE", "positive", ("UnitManufacturingCostUSD", "WholesalePriceUSD", "RecommendedRetailPriceUSD"))]),
    "02_manufacturing_data": Source(
        "02_manufacturing_data", "Manufacturing", "stg_manufacturing_data", ("ManufacturingOrderID",), 2, "batch", False,
        [Rule("NULL_KEY", "not_null", ("ManufacturingOrderID", "ProductionBatchID", "MedicineID")),
         Rule("UNKNOWN_MEDICINE", "fk_medicine", ("MedicineID",)),
         Rule("NEGATIVE_QUANTITY", "non_negative", ("ActualQuantityPacks", "ReleasedQuantityPacks"))]),
    "03_manufacturer_delivery_data": Source(
        "03_manufacturer_delivery_data", "Manufacturer delivery", "stg_manufacturer_delivery_data",
        ("ShipmentID", "ShipmentLineNumber"), 3, "batch", False,
        [Rule("NULL_KEY", "not_null", ("ShipmentID", "ShipmentLineNumber", "MedicineID")),
         Rule("UNKNOWN_MEDICINE", "fk_medicine", ("MedicineID",)),
         Rule("NEGATIVE_QUANTITY", "non_negative", ("ShippedQuantityPacks", "ReceivedQuantityPacks"))]),
    "04_warehouse_data": Source(
        "04_warehouse_data", "Warehouse", "stg_warehouse_data", ("InventorySnapshotID",), 4, "batch", False,
        [Rule("NULL_KEY", "not_null", ("InventorySnapshotID", "WarehouseLotID", "MedicineID")),
         Rule("UNKNOWN_MEDICINE", "fk_medicine", ("MedicineID",)),
         Rule("NEGATIVE_QUANTITY", "non_negative", ("ClosingQuantityPacks",))]),
    "05_pharmacy_sales_data": Source(
        "05_pharmacy_sales_data", "Pharmacy sales", "stg_pharmacy_sales_data", ("SalesTransactionID",), 5, "stream", True,
        [Rule("NULL_KEY", "not_null", ("SalesTransactionID", "MedicineID", "PharmacyID")),
         Rule("UNKNOWN_MEDICINE", "fk_medicine", ("MedicineID",)),
         Rule("NON_POSITIVE_PRICE", "positive", ("UnitRetailPriceUSD", "QuantitySoldPacks")),
         Rule("AMOUNT_MISMATCH", "equation", ("GrossSalesAmountUSD", "DiscountAmountUSD", "NetSalesAmountUSD"),
              {"tolerance": 0.02})]),
    "06_hospital_demand_data": Source(
        "06_hospital_demand_data", "Hospital demand", "stg_hospital_demand_data", ("HospitalDemandRecordID",), 6, "batch", True,
        [Rule("NULL_KEY", "not_null", ("HospitalDemandRecordID", "HospitalID", "MedicineID")),
         Rule("UNKNOWN_MEDICINE", "fk_medicine", ("MedicineID",)),
         Rule("NEGATIVE_QUANTITY", "non_negative", ("ForecastDemandPacks", "RequestedQuantityPacks",
                                                    "ReceivedQuantityPacks", "ConsumedQuantityPacks"))]),
    "07_iot_streaming_data": Source(
        "07_iot_streaming_data", "IoT telemetry", "stg_iot_streaming_data", ("SensorReadingID",), 7, "batch", False,
        [Rule("NULL_KEY", "not_null", ("SensorReadingID", "DeviceID")),
         Rule("SENSOR_OUT_OF_RANGE", "range", ("TemperatureC",), {"min": -50, "max": 80})]),
}


def source_for_name(name: str):
    """Map a file or sheet name such as 'R1_06_hospital_demand_data.xlsx' to its Source."""
    for key, src in SOURCES.items():
        if key in name:
            return src
    return None
