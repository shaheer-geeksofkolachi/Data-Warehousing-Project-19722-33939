"""Oversampling data generator: synthesises one new month of data for ALL 7 source files.

Method (per refresh month, e.g. R1 = 2026-01, R2 = 2026-02)
-----------------------------------------------------------
* Statistical bootstrap: rows are resampled from recent history (same calendar month last year where
  seasonality matters, otherwise the last 12 months), then dates are shifted into the target month and numeric
  measures get small multiplicative noise and a year-on-year growth factor measured from the history itself.
* Lineage is preserved end-to-end: new manufacturing batch -> shipment lines -> warehouse lots -> pharmacy sales,
  so every new sale still points to a real lot/batch whose expiry is after the sale date.
* Business identities are recomputed, never copied (Net = Gross - Discount, COGS = qty x wholesale, closing stock
  = opening + received - dispensed - wastage, OTIF = on-time AND in-full, received = shipped - damaged - short ...).
* Slowly-changing-dimension triggers: R1 changes 2 medicine prices; R2 changes 3 prices, renames one brand (Type-1),
  launches 2 new medicines and expands one hospital (beds/ICU -> Type-2 history in dim_hospital).
* Data-quality injection: ~1% malformed rows (zero price, unknown SKU, bad date, amount mismatch, duplicate key ...)
  so the validation layer and the "malformed / rejected" dashboard counters are exercised.
* History is base files + all earlier refreshes, so R2 continues R1 (IDs, stock positions, lots).
"""
import json
import calendar
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import SOURCE_DIR, GENERATED_DIR, LANDING_DIR, STREAM_QUEUE_DIR, REFRESHES
from ..sources import SOURCES

SRC_FILES = {k: f"{k}.csv" for k in SOURCES}
ORDER = ["01_master_data", "02_manufacturing_data", "03_manufacturer_delivery_data", "04_warehouse_data",
         "05_pharmacy_sales_data", "06_hospital_demand_data", "07_iot_streaming_data"]
DIRTY_RATE = {"02_manufacturing_data": 0.01, "03_manufacturer_delivery_data": 0.01, "04_warehouse_data": 0.002,
              "05_pharmacy_sales_data": 0.01, "06_hospital_demand_data": 0.005, "07_iot_streaming_data": 0.001}
YOY_GROWTH = 1.05            # cross-checked against history: hospital consumption +3.5%/yr, sales +8%/yr
BULK_POSTING_GAP_DAYS = 240  # sales posted >240 days after delivery on 1 Jan are the year-start bulk postings

# ----------------------------------------------------------------------------------------------- SCD scenarios
SCENARIOS = {
    "R1": {"price_changes": {"MED010": 1.08, "MED024": 1.05}, "brand_renames": {}, "new_medicines": [],
           "hospital_changes": {}},
    "R2": {"price_changes": {"MED007": 1.06, "MED019": 1.04, "MED037": 0.97},
           "brand_renames": {"MED045": "ClariLoperamide45"},
           "new_medicines": [
               dict(MedicineID="MED064", clone_of="MED009", GenericName="Tirzepatide", FictionalBrandName="ClariTirzepa64",
                    ATCCode="A10BX16", TherapeuticClass="GIP/GLP-1 receptor agonist", Strength="5 mg/dose", PackSize=4,
                    UnitManufacturingCostUSD=52.0, WholesalePriceUSD=105.0, RecommendedRetailPriceUSD=138.6,
                    PrimarySupplierID="SUP010", PrimaryPlantID="PLT03", volume_factor=0.6),
               dict(MedicineID="MED065", clone_of="MED017", GenericName="Apixaban", FictionalBrandName="ClariApixaba65",
                    ATCCode="B01AF02", TherapeuticClass="Direct factor Xa inhibitor", Strength="5 mg", PackSize=60,
                    UnitManufacturingCostUSD=2.6, WholesalePriceUSD=9.8, RecommendedRetailPriceUSD=12.94,
                    PrimarySupplierID="SUP011", PrimaryPlantID="PLT04", volume_factor=0.5)],
           "hospital_changes": {"HOSP003": {"LicensedBeds": 360, "ICUBeds": 46}}},
}


# ----------------------------------------------------------------------------------------------- helpers
def gs1_check_digit(body: str) -> str:
    total = sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return str((10 - total % 10) % 10)


def max_seq(series: pd.Series, pattern=r"(\d+)$") -> int:
    s = series.dropna().astype(str).str.extract(pattern)[0].dropna()
    return int(s.astype(int).max()) if len(s) else 0


def iso_z(ts: pd.Series) -> pd.Series:
    return pd.Series(pd.to_datetime(ts, utc=True)).dt.strftime("%Y-%m-%dT%H:%M:%SZ").values


def dstr(d) -> pd.Series:
    return pd.Series(pd.to_datetime(d)).dt.strftime("%Y-%m-%d").values


class Generator:
    def __init__(self, tag: str, seed: int = None):
        self.tag = tag
        self.month = REFRESHES[tag]["month"]
        self.m_start = pd.Timestamp(self.month + "-01")
        self.days = calendar.monthrange(self.m_start.year, self.m_start.month)[1]
        self.m_end = self.m_start + pd.Timedelta(days=self.days - 1)
        self.rng = np.random.default_rng(seed if seed is not None else int(self.m_start.strftime("%Y%m")))
        self.sc = SCENARIOS.get(tag, {"price_changes": {}, "brand_renames": {}, "new_medicines": [],
                                      "hospital_changes": {}})
        self.hist = self._load_history()
        self.out, self.dirty = {}, {}

    # ------------------------------------------------------------------ history = base + earlier refreshes
    def _load_history(self):
        hist = {}
        for k in ORDER:
            f = SOURCE_DIR / SRC_FILES[k]
            df = pd.read_csv(f, dtype={"GTIN14": str, "SSCC18": str}, low_memory=False)
            frames = [df]
            for tag in sorted(REFRESHES):
                if tag >= self.tag:
                    break
                p = GENERATED_DIR / tag / "_typed" / f"{k}.parquet"
                if p.exists():
                    frames.append(pd.read_parquet(p))
            h = pd.concat(frames, ignore_index=True)
            if "SSCC18" in h:
                h["SSCC18"] = h["SSCC18"].astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(18)
            hist[k] = h
        return hist

    def noise(self, n, sd=0.04):
        return self.rng.normal(1.0, sd, n).clip(0.85, 1.15)

    def rand_dates_in_month(self, n):
        return self.m_start + pd.to_timedelta(self.rng.integers(0, self.days, n), unit="D")

    # ------------------------------------------------------------------ 01 master
    def gen_master(self):
        h = self.hist["01_master_data"].copy()
        h["_eff"] = pd.to_datetime(h["EffectiveFromDate"])
        cur = h.sort_values("_eff").groupby("MedicineID").tail(1).drop(columns="_eff").reset_index(drop=True)
        eff = self.m_start.strftime("%Y-%m-%d")
        for med, f in self.sc["price_changes"].items():
            i = cur.index[cur.MedicineID == med]
            for c in ("UnitManufacturingCostUSD", "WholesalePriceUSD", "RecommendedRetailPriceUSD"):
                cur.loc[i, c] = (cur.loc[i, c] * f).round(2)
            cur.loc[i, "EffectiveFromDate"] = eff
        for med, name in self.sc["brand_renames"].items():
            i = cur.index[cur.MedicineID == med]
            cur.loc[i, "FictionalBrandName"] = name
            cur.loc[i, "EffectiveFromDate"] = eff
        for nm in self.sc["new_medicines"]:
            base = cur[cur.MedicineID == nm["clone_of"]].iloc[0].copy()
            for k, v in nm.items():
                if k in base.index:
                    base[k] = v
            num = int(nm["MedicineID"][3:])
            cat = base["InternalSKU"].split("-")[1]
            base["InternalSKU"] = f"SKU-{cat}-{num:04d}"
            body = f"0950600000{num:03d}"
            base["GTIN14"] = body + gs1_check_digit(body)
            base["EffectiveFromDate"] = eff
            cur = pd.concat([cur, base.to_frame().T], ignore_index=True)
        self.master_now = cur.copy()
        for c in ("PackSize", "StorageMinTempC", "StorageMaxTempC", "MaxHumidityPct", "ShelfLifeMonths"):
            self.master_now[c] = pd.to_numeric(self.master_now[c])
        self.out["01_master_data"] = cur  # full snapshot; unchanged rows are skipped as duplicates by staging
        self.dirty["01_master_data"] = 0

    # ------------------------------------------------------------------ 02 manufacturing
    def gen_manufacturing(self):
        h = self.hist["02_manufacturing_data"]
        od = pd.to_datetime(h.OrderDate)
        last12 = h[od >= od.max() - pd.Timedelta(days=365)]
        n = int(round(len(last12) / 12 * YOY_GROWTH * self.rng.uniform(0.95, 1.05)))
        smp = last12.sample(n, replace=True, random_state=int(self.rng.integers(1e9))).reset_index(drop=True)
        new_od = self.rand_dates_in_month(n)
        shift = new_od - pd.to_datetime(smp.OrderDate)
        launch = []
        for nm in self.sc["new_medicines"]:   # launch batches: produced ahead of launch, released before month start
            t = h[(h.MedicineID == nm["clone_of"])].tail(1).copy()
            t["MedicineID"] = nm["MedicineID"]
            launch.append((t, (self.m_start - pd.Timedelta(days=12)) - pd.to_datetime(t.BatchReleaseDate.iloc[0])))
        smp_all = [smp]
        shifts = [shift]
        for t, s in launch:
            smp_all.append(t.reset_index(drop=True))
            shifts.append(pd.Series([s] * len(t)))
        df = pd.concat(smp_all, ignore_index=True)
        shift = pd.concat(shifts, ignore_index=True)
        for c in ["OrderDate", "PlannedStartDate", "PlannedCompletionDate", "ActualStartDate", "ActualCompletionDate",
                  "QualitySampleDate", "BatchReleaseDate"]:
            df[c] = dstr(pd.to_datetime(df[c]) + shift.values)
        m = self.master_now.set_index("MedicineID")
        df["GTIN14"] = df.MedicineID.map(m.GTIN14)
        df["PlantID"] = df.MedicineID.map(m.PrimaryPlantID)
        df["StandardBatchSizePacks"] = df.MedicineID.map(m.StandardBatchSizePacks).astype(int)
        df["ManufacturingLeadTimeDays"] = df.MedicineID.map(m.ManufacturingLeadTimeDays).astype(int)
        f = self.noise(len(df), 0.05)
        df["PlannedQuantityPacks"] = (df.PlannedQuantityPacks * f / 100).round().astype(int) * 100
        df["ActualQuantityPacks"] = (df.ActualQuantityPacks * f * self.noise(len(df), 0.01)).round().astype(int)
        rej_ratio = (df.RejectedQuantityPacks / df.ActualQuantityPacks.replace(0, np.nan)).fillna(0.02)
        df["RejectedQuantityPacks"] = (df.ActualQuantityPacks * rej_ratio).round().astype(int).clip(lower=1)
        rejected = df.BatchStatus.eq("Rejected")
        df.loc[rejected, "RejectedQuantityPacks"] = df.loc[rejected, "ActualQuantityPacks"]
        cond = df.QualityTestStatus.eq("Conditional pass")
        hold = (df.ActualQuantityPacks * 0.005).round().astype(int)
        df["ReleasedQuantityPacks"] = df.ActualQuantityPacks - df.RejectedQuantityPacks - np.where(cond, hold, 0)
        df.loc[rejected, "ReleasedQuantityPacks"] = 0
        df["YieldPct"] = (df.ReleasedQuantityPacks / df.ActualQuantityPacks * 100).round(2)
        shelf = df.MedicineID.map(m.ShelfLifeMonths).astype(int)
        df["ExpiryDate"] = [dstr(pd.Series([pd.Timestamp(r) + pd.DateOffset(months=int(s))]))[0]
                            for r, s in zip(df.BatchReleaseDate, shelf)]
        unit = df.MedicineID.map(m.UnitManufacturingCostUSD).astype(float)
        scale = f
        df["LaborCostUSD"] = (df.LaborCostUSD * scale).round(2)
        df["MaterialCostUSD"] = (df.MaterialCostUSD * scale * (unit / df.MedicineID.map(
            self.hist["01_master_data"].groupby("MedicineID").UnitManufacturingCostUSD.first()).fillna(unit))).round(2)
        df["OverheadCostUSD"] = (df.OverheadCostUSD * scale).round(2)
        df["TotalManufacturingCostUSD"] = (df.LaborCostUSD + df.MaterialCostUSD + df.OverheadCostUSD).round(2)
        df["CostPerReleasedPackUSD"] = (df.TotalManufacturingCostUSD / df.ReleasedQuantityPacks.replace(0, np.nan)).round(4)
        seq0 = max_seq(h.ManufacturingOrderID)
        yy = self.m_start.year
        seqs = np.arange(seq0 + 1, seq0 + 1 + len(df))
        df["ManufacturingOrderID"] = [f"MO-{yy}-{s:05d}" for s in seqs]
        df["ProductionBatchID"] = [f"B{str(yy)[2:]}-{p}-{s:06d}" for p, s in zip(df.PlantID, seqs)]
        df["RecordCreatedTimestampUTC"] = iso_z(pd.to_datetime(df.BatchReleaseDate) + pd.Timedelta(days=1, hours=6))
        self.mfg_new = df.copy()
        self.out["02_manufacturing_data"] = df

    # ------------------------------------------------------------------ 03 manufacturer delivery
    def gen_delivery(self):
        h = self.hist["03_manufacturer_delivery_data"]
        mf_hist = self.hist["02_manufacturing_data"][["ProductionBatchID", "BatchReleaseDate"]]
        hh = h.merge(mf_hist, on="ProductionBatchID")
        rel = self.mfg_new[self.mfg_new.BatchStatus.eq("Released")]
        lines = []
        for _, b in rel.iterrows():
            cand = hh[hh.MedicineID == b.MedicineID] if (hh.MedicineID == b.MedicineID).any() else hh
            tb = cand.ProductionBatchID.sample(1, random_state=int(self.rng.integers(1e9))).iloc[0]
            tpl = hh[hh.ProductionBatchID == tb].copy()
            tot = tpl.ShippedQuantityPacks.sum()
            # ship ~ the same share of the released quantity as the template batch did
            share = min(tot / max(self.hist["02_manufacturing_data"].set_index("ProductionBatchID")
                                  .ReleasedQuantityPacks.get(tb, tot), 1), 1.0)
            ship_tot = b.ReleasedQuantityPacks * share
            shift = pd.Timestamp(b.BatchReleaseDate) - pd.to_datetime(tpl.BatchReleaseDate)
            for c in ("DispatchDate", "PromisedDeliveryDate", "ActualDeliveryDate"):
                tpl[c] = pd.to_datetime(tpl[c]) + shift
            q = (tpl.ShippedQuantityPacks / tot * ship_tot).round().astype(int)
            ratio = q / tpl.ShippedQuantityPacks
            tpl["ShippedQuantityPacks"] = q
            tpl["DamagedQuantityPacks"] = (tpl.DamagedQuantityPacks * ratio).round().astype(int)
            tpl["ShortQuantityPacks"] = (tpl.ShortQuantityPacks * ratio).round().astype(int)
            tpl["FreightCostUSD"] = (tpl.FreightCostUSD * ratio * self.noise(len(tpl), 0.03)).round(2)
            for c in ("ManufacturingOrderID", "ProductionBatchID", "MedicineID", "GTIN14"):
                tpl[c] = b[c]
            lines.append(tpl)
        df = pd.concat(lines, ignore_index=True).drop(columns=["BatchReleaseDate"])
        m = self.master_now.set_index("MedicineID")
        df["ColdChainRequiredFlag"] = df.MedicineID.map(m.ColdChainFlag)
        df["TemperatureMonitoringRequiredFlag"] = df["ColdChainRequiredFlag"]
        df["ReceivedQuantityPacks"] = df.ShippedQuantityPacks - df.DamagedQuantityPacks - df.ShortQuantityPacks
        df["InFullDeliveryFlag"] = np.where((df.DamagedQuantityPacks + df.ShortQuantityPacks) == 0, "Y", "N")
        df["OnTimeDeliveryFlag"] = np.where(df.ActualDeliveryDate <= df.PromisedDeliveryDate, "Y", "N")
        df["OTIFFlag"] = np.where((df.InFullDeliveryFlag == "Y") & (df.OnTimeDeliveryFlag == "Y"), "Y", "N")
        df.loc[df.OTIFFlag == "Y", "DeliveryExceptionReason"] = np.nan
        df["FreightCostPerPackUSD"] = (df.FreightCostUSD / df.ShippedQuantityPacks).round(4)
        # group lines into shipments: same dispatch day + lane
        df = df.sort_values(["DispatchDate", "OriginPlantID", "DestinationWarehouseID"]).reset_index(drop=True)
        grp = df.groupby(["DispatchDate", "OriginPlantID", "DestinationWarehouseID", "TransportMode"], sort=False).ngroup()
        s0 = max_seq(h.ShipmentID)
        df["ShipmentID"] = [f"MFG-SHP-{s0 + 1 + g:07d}" for g in grp]
        df["ShipmentLineNumber"] = df.groupby("ShipmentID").cumcount() + 1
        sscc_seq = max_seq(h.SSCC18.astype(str).str[:-1]) % 100000
        seal0 = max_seq(h.SealNumber)
        ss, se = {}, {}
        for i, sid in enumerate(df.ShipmentID.unique()):
            body = f"0950600100080{sscc_seq + 1 + i:04d}"
            ss[sid] = body + gs1_check_digit(body)
            se[sid] = f"SEAL-{seal0 + 1 + i:08d}"
        df["SSCC18"] = df.ShipmentID.map(ss)
        df["SealNumber"] = df.ShipmentID.map(se)
        for c in ("DispatchDate", "PromisedDeliveryDate", "ActualDeliveryDate"):
            df[c] = dstr(df[c])
        self.dl_new = df.copy()
        self.out["03_manufacturer_delivery_data"] = df

    # ------------------------------------------------------------------ 04 warehouse (daily lot snapshots)
    def gen_warehouse(self):
        h = self.hist["04_warehouse_data"]
        m = self.master_now.set_index("MedicineID")
        h_last = h[h.SnapshotDate == h.SnapshotDate.max()]
        cont = h_last[(h_last.ClosingQuantityPacks > 0) & (h_last.DaysToExpiry > 0)].copy()
        daily_ship = h[h.ShippedQuantityPacks > 0].groupby("MedicineID").ShippedQuantityPacks.mean()
        ship_prob = (h.ShippedQuantityPacks > 0).groupby(h.MedicineID).mean()
        zones = h.groupby(["WarehouseID", "StorageCondition"]).StorageZoneID.unique()
        bins = h.groupby("StorageZoneID").AllocatedBinCapacityPacks.median()
        whcap = h.groupby("WarehouseID").WarehouseTotalCapacityPacks.first()
        whinfo = h.groupby("WarehouseID")[["WarehouseGLN", "WarehouseName"]].first()
        lot0 = max_seq(h.WarehouseLotID)
        lots = []
        for _, r in cont.iterrows():
            lots.append(dict(lot=r.WarehouseLotID, ship=r.InboundShipmentID, line=r.InboundShipmentLineNumber,
                             batch=r.ProductionBatchID, med=r.MedicineID, wh=r.WarehouseID, zone=r.StorageZoneID,
                             cond=r.StorageCondition, expiry=pd.Timestamp(r.BatchExpiryDate),
                             start=self.m_start, opening=int(r.ClosingQuantityPacks), received=0, damaged=0))
        mf = pd.concat([self.hist["02_manufacturing_data"], self.mfg_new]).drop_duplicates(
            "ProductionBatchID", keep="last").set_index("ProductionBatchID")
        # deliveries that physically arrive in the target month and have no lot yet: lines generated by this
        # refresh plus earlier lines (base / previous refresh) that were still in transit at the last snapshot
        seen = set(zip(h.InboundShipmentID, h.InboundShipmentLineNumber))
        cand = pd.concat([self.hist["03_manufacturer_delivery_data"], self.dl_new], ignore_index=True)
        cand = cand[[(a, b) not in seen for a, b in zip(cand.ShipmentID, cand.ShipmentLineNumber)]]
        cand = cand[pd.to_datetime(cand.ActualDeliveryDate) <= self.m_end]
        cand = cand.drop_duplicates(["ShipmentID", "ShipmentLineNumber"]).reset_index(drop=True)
        for i, r in cand.iterrows():
            cond = "2-8 C" if m.loc[r.MedicineID, "ColdChainFlag"] == "Y" else "15-25 C"
            wh = r.DestinationWarehouseID if (r.DestinationWarehouseID, cond) in zones.index else "WH006"
            zl = zones[(wh, cond)]
            lots.append(dict(lot=f"WHLOT-{lot0 + 1 + i:07d}", ship=r.ShipmentID, line=r.ShipmentLineNumber,
                             batch=r.ProductionBatchID, med=r.MedicineID, wh=wh,
                             zone=zl[self.rng.integers(len(zl))], cond=cond,
                             expiry=pd.Timestamp(mf.loc[r.ProductionBatchID, "ExpiryDate"]),
                             start=pd.Timestamp(r.ActualDeliveryDate), opening=0,
                             received=int(r.ReceivedQuantityPacks),
                             damaged=int(self.rng.integers(0, 15)) if self.rng.random() < 0.3 else 0))
        rows = []
        for L in lots:
            if L["start"] > self.m_end:
                continue
            opening = L["opening"]
            mu = daily_ship.get(L["med"], 80) * L.get("vf", 1.0)
            p = ship_prob.get(L["med"], 0.9)
            d = max(L["start"], self.m_start)
            first = True
            while d <= self.m_end:
                days_to_exp = (L["expiry"] - d).days
                receipt = first and L["received"] > 0
                rec = L["received"] if receipt else 0
                put = rec - L["damaged"] if receipt else 0
                quarantine = put if receipt else 0
                shipped = picked = tin = tout = dmg = exp = adj = 0
                if not receipt and opening > 0:
                    if days_to_exp <= 0:
                        exp = opening
                    elif self.rng.random() < p:
                        shipped = int(min(opening, max(1, self.rng.normal(mu, mu * 0.35))))
                        picked = int(round(shipped * self.rng.uniform(0.9, 1.0)))
                    if self.rng.random() < 0.004:
                        tout = int(min(opening - shipped, self.rng.integers(20, 120)))
                    if self.rng.random() < 0.002:
                        dmg = int(min(opening - shipped - tout, self.rng.integers(1, 20)))
                cc = (not receipt) and self.rng.random() < 0.035
                if cc:
                    adj = int(self.rng.choice([-2, -1, 1, 2], p=[.2, .4, .3, .1]))
                closing = max(0, opening + put + tin - shipped - tout - dmg - exp + adj) if not receipt else put
                reserved = int(closing * self.rng.uniform(0.05, 0.15)) if closing and not receipt else 0
                available = 0 if receipt else max(closing - reserved, 0)
                status = ("Quarantined" if receipt else "Expired" if days_to_exp <= 0 else
                          "Depleted" if closing == 0 else "Near expiry" if days_to_exp <= 180 else "Available")
                activity = ("Receive and putaway" if receipt else "Expiry disposal pending" if exp else
                            "Cycle count" if cc and shipped == 0 else "Pick and dispatch" if shipped else
                            "Storage monitoring")
                rows.append(dict(
                    SnapshotDate=d, WarehouseLotID=L["lot"], InboundShipmentID=L["ship"],
                    InboundShipmentLineNumber=L["line"], ProductionBatchID=L["batch"], MedicineID=L["med"],
                    GTIN14=m.loc[L["med"], "GTIN14"], WarehouseID=L["wh"], WarehouseGLN=whinfo.loc[L["wh"], "WarehouseGLN"],
                    WarehouseName=whinfo.loc[L["wh"], "WarehouseName"], StorageZoneID=L["zone"],
                    StorageCondition=L["cond"], BatchExpiryDate=L["expiry"].strftime("%Y-%m-%d"),
                    DaysToExpiry=max(days_to_exp, 0), InventoryStatus=status, OpeningQuantityPacks=opening,
                    ReceivedQuantityPacks=rec, PutawayQuantityPacks=put, QuarantineQuantityPacks=quarantine,
                    AvailableQuantityPacks=available, ReservedQuantityPacks=reserved, PickedQuantityPacks=picked,
                    ShippedQuantityPacks=shipped, TransferInQuantityPacks=tin, TransferOutQuantityPacks=tout,
                    DamagedQuantityPacks=L["damaged"] if receipt else dmg, ExpiredQuantityPacks=exp,
                    AdjustmentQuantityPacks=adj, ClosingQuantityPacks=closing,
                    StockoutFlag="Y" if closing == 0 and opening > 0 and not exp else "N",
                    NearExpiryFlag="Y" if 0 < days_to_exp <= 180 else "N",
                    FEFOComplianceFlag="N" if shipped and self.rng.random() < 0.036 else "Y",
                    CycleCountPerformedFlag="Y" if cc else "N", CycleCountVariancePacks=adj if cc else 0,
                    DockToStockHours=round(self.rng.uniform(5, 15), 1) if receipt else np.nan,
                    OrderPickingAccuracyPct=round(self.rng.uniform(97.5, 100), 2) if picked else np.nan,
                    AllocatedBinCapacityPacks=int(bins.get(L["zone"], 11000)),
                    WarehouseTotalCapacityPacks=int(whcap.loc[L["wh"]]), WarehouseActivityStatus=activity,
                    SourceSystem="WMS_DAILY_SNAPSHOT"))
                opening = closing
                first = False
                if closing == 0:
                    break
                d += pd.Timedelta(days=1)
        df = pd.DataFrame(rows).sort_values(["SnapshotDate", "WarehouseLotID"]).reset_index(drop=True)
        df["SnapshotDate"] = dstr(df.SnapshotDate)
        inv0 = max_seq(h.InventorySnapshotID)
        df.insert(0, "InventorySnapshotID", [f"INV-{inv0 + 1 + i:09d}" for i in range(len(df))])
        self.wh_new = df.copy()
        # lot pool for sales lineage = every lot that has ever been on hand (history + this month)
        hl = h.assign(_d=pd.to_datetime(h.SnapshotDate)).groupby("WarehouseLotID").agg(
            batch=("ProductionBatchID", "first"), med=("MedicineID", "first"), wh=("WarehouseID", "first"),
            expiry=("BatchExpiryDate", "first"), start=("_d", "min"), last_closing=("ClosingQuantityPacks", "last"))
        hl = hl.reset_index().rename(columns={"WarehouseLotID": "lot"})
        hl["expiry"] = pd.to_datetime(hl.expiry)
        new_lots = pd.DataFrame(lots)
        new_lots = new_lots[new_lots.start <= self.m_end][["lot", "batch", "med", "wh", "expiry", "start"]]
        self.lots_now = pd.concat([hl.drop(columns="last_closing"), new_lots]).drop_duplicates("lot", keep="last")
        self.out["04_warehouse_data"] = df

    # ------------------------------------------------------------------ 05 pharmacy sales
    def gen_sales(self):
        h = self.hist["05_pharmacy_sales_data"]
        sd = pd.to_datetime(h.SalesDate)
        gap = (sd - pd.to_datetime(h.PharmacyDeliveryDate)).dt.days
        normal = h[~((sd.dt.month == 1) & (sd.dt.day == 1) & (gap > BULK_POSTING_GAP_DAYS))]
        nsd = pd.to_datetime(normal.SalesDate)
        recent = normal[nsd > nsd.max() - pd.Timedelta(days=92)]
        per_day = len(recent) / 92
        n = int(round(per_day * self.days * YOY_GROWTH * self.rng.uniform(0.97, 1.03)))
        last12 = normal[nsd > nsd.max() - pd.Timedelta(days=365)]
        smp = last12.sample(n, replace=True, random_state=int(self.rng.integers(1e9))).reset_index(drop=True)
        new_meds = []
        for nm in self.sc["new_medicines"]:
            src_rows = last12[last12.MedicineID == nm["clone_of"]]
            k = int(round(len(src_rows) / 12 * nm["volume_factor"]))
            c = src_rows.sample(k, replace=True, random_state=int(self.rng.integers(1e9))).copy()
            c["MedicineID"] = nm["MedicineID"]
            new_meds.append(c)
        df = pd.concat([smp] + new_meds, ignore_index=True)
        m = self.master_now.set_index("MedicineID")
        # dates: random day in target month, original time-of-day
        day = self.rand_dates_in_month(len(df))
        launch_meds = {nm["MedicineID"] for nm in self.sc["new_medicines"]}
        if launch_meds:  # new medicines only sell after their launch stock has arrived
            lm = df.MedicineID.isin(launch_meds).values
            day = pd.DatetimeIndex(np.where(lm, self.m_start + pd.to_timedelta(
                self.rng.integers(5, self.days, len(df)), unit="D"), day))
        tod = pd.to_datetime(df.SalesTimestampUTC, utc=True)
        tod = tod - tod.dt.normalize()
        df["SalesDate"] = dstr(day)
        df["SalesTimestampUTC"] = iso_z(pd.Series(day).dt.tz_localize("UTC") + tod.values)
        df["GTIN14"] = df.MedicineID.map(m.GTIN14)
        df["GenericName"] = df.MedicineID.map(m.GenericName)
        df["DiseaseCategory"] = df.MedicineID.map(m.DiseaseCategory)
        # lineage: pick a warehouse lot holding this medicine on the sale date (received before, expiring after)
        lots = self.lots_now.copy()
        lot_pick, batch_pick, wh_pick, exp_pick = [], [], [], []
        by_med = {k: g for k, g in lots.groupby("med")}
        for med, sdate in zip(df.MedicineID, pd.to_datetime(df.SalesDate)):
            g = by_med.get(med)
            if g is not None:
                onhand = g[g.start <= sdate - pd.Timedelta(days=1)]
                ok = onhand[onhand.expiry > sdate + pd.Timedelta(days=30)]
                if ok.empty:
                    ok = onhand[onhand.expiry > sdate]
                if ok.empty:
                    ok = onhand if not onhand.empty else g
                r = ok.iloc[self.rng.integers(len(ok))]
                lot_pick.append(r.lot); batch_pick.append(r.batch); wh_pick.append(r.wh); exp_pick.append(r.expiry)
            else:
                lot_pick.append(None); batch_pick.append(None); wh_pick.append(None); exp_pick.append(pd.NaT)
        miss = pd.isna(pd.Series(exp_pick))
        df["WarehouseLotID"] = np.where(miss, df.WarehouseLotID, lot_pick)
        df["ProductionBatchID"] = np.where(miss, df.ProductionBatchID, batch_pick)
        df["SourceWarehouseID"] = np.where(miss, df.SourceWarehouseID, wh_pick)
        exp = pd.Series(exp_pick).fillna(pd.to_datetime(df.BatchExpiryDate))
        df["BatchExpiryDate"] = dstr(exp)
        df["DaysToExpiryAtSale"] = (exp - pd.to_datetime(df.SalesDate)).dt.days
        # pharmacy replenishment deliveries: remap template delivery ids to new ones
        d0 = max_seq(h.PharmacyDeliveryID)
        key = df.PharmacyDeliveryID + "|" + df.MedicineID
        codes, uniq = pd.factorize(key)
        df["PharmacyDeliveryID"] = [f"PHD-{d0 + 1 + c:07d}" for c in codes]
        df["PharmacyOrderID"] = [f"PHO-{d0 + 1 + c:07d}" for c in codes]
        first_sale = pd.to_datetime(df.SalesDate).groupby(codes).transform("min")
        lead = pd.Series(self.rng.integers(1, 15, len(uniq)))[codes].values
        df["PharmacyDeliveryDate"] = dstr(first_sale - pd.to_timedelta(lead, unit="D"))
        # prices from the CURRENT master (SCD changes flow into sales)
        price = df.MedicineID.map(m.RecommendedRetailPriceUSD).astype(float)
        whs = df.MedicineID.map(m.WholesalePriceUSD).astype(float)
        disc_rate = (df.DiscountAmountUSD / df.GrossSalesAmountUSD).fillna(0)
        tax_rate = (df.SalesTaxUSD / df.NetSalesAmountUSD).fillna(0.06)
        df["UnitRetailPriceUSD"] = price.round(2)
        df["GrossSalesAmountUSD"] = (df.QuantitySoldPacks * price).round(2)
        df["DiscountAmountUSD"] = (df.GrossSalesAmountUSD * disc_rate).round(2)
        df["NetSalesAmountUSD"] = (df.GrossSalesAmountUSD - df.DiscountAmountUSD).round(2)
        df["SalesTaxUSD"] = (df.NetSalesAmountUSD * tax_rate).round(2)
        df["CostOfGoodsSoldUSD"] = (df.QuantitySoldPacks * whs).round(2)
        df["GrossMarginUSD"] = (df.NetSalesAmountUSD - df.CostOfGoodsSoldUSD).round(2)
        df["PrescriptionRequiredFlag"] = np.where(df.MedicineID.map(m.PrescriptionClass) == "Prescription", "Y",
                                                  df.PrescriptionRequiredFlag)
        s0 = max_seq(h.SalesTransactionID)
        df = df.sort_values("SalesTimestampUTC").reset_index(drop=True)
        df["SalesTransactionID"] = [f"SALE-{s0 + 1 + i:09d}" for i in range(len(df))]
        rx0 = max_seq(h.PrescriptionID)
        has_rx = df.PrescriptionID.notna()
        df.loc[has_rx, "PrescriptionID"] = [f"RX-{rx0 + 1 + i:09d}" for i in range(int(has_rx.sum()))]
        self.out["05_pharmacy_sales_data"] = df

    # ------------------------------------------------------------------ 06 hospital demand
    def gen_hospital(self):
        h = self.hist["06_hospital_demand_data"]
        hm = pd.to_datetime(h.DemandMonth)
        last_month = hm.max()
        prev = h[hm == last_month].set_index(["HospitalID", "MedicineID"])
        same_month_ly = h[hm == self.m_start - pd.DateOffset(years=1)]
        if same_month_ly.empty:
            same_month_ly = h[hm == last_month]
        tpl = same_month_ly.copy()
        clones = []
        for nm in self.sc["new_medicines"]:
            c = tpl[tpl.MedicineID == nm["clone_of"]].copy()
            c["MedicineID"] = nm["MedicineID"]
            c["_vf"] = nm["volume_factor"]
            clones.append(c)
        df = pd.concat([tpl] + clones, ignore_index=True)
        df["_vf"] = df.get("_vf", pd.Series(1.0, index=df.index)).fillna(1.0)
        m = self.master_now.set_index("MedicineID")
        df["DemandMonth"] = self.m_start.strftime("%Y-%m-%d")
        df["GTIN14"] = df.MedicineID.map(m.GTIN14)
        df["GenericName"] = df.MedicineID.map(m.GenericName)
        df["DiseaseCategory"] = df.MedicineID.map(m.DiseaseCategory)
        df["ICD10Category"] = df.MedicineID.map(m.ICD10Category)
        latest_attrs = h[hm == last_month].groupby("HospitalID")[["LicensedBeds", "ICUBeds"]].first()
        df["LicensedBeds"] = df.HospitalID.map(latest_attrs.LicensedBeds)
        df["ICUBeds"] = df.HospitalID.map(latest_attrs.ICUBeds)
        for hid, ch in self.sc["hospital_changes"].items():
            for k, v in ch.items():
                df.loc[df.HospitalID == hid, k] = v
        growth = YOY_GROWTH ** ((self.m_start - pd.to_datetime(tpl.DemandMonth.iloc[0])).days / 365)
        n = len(df)
        bed_f = df.LicensedBeds / df.HospitalID.map(h.groupby("HospitalID").LicensedBeds.first())
        for c in ("EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters"):
            df[c] = (df[c] * growth * bed_f * self.noise(n, 0.03)).round().astype(int)
        cons = (df.ConsumedQuantityPacks * growth * bed_f * df._vf * self.noise(n, 0.05)).round().clip(lower=1)
        bias = self.rng.normal(0.011, 0.05, n)               # forecasts keep under-shooting consumption (~1%)
        df["ForecastDemandPacks"] = (cons * (1 - bias)).round().clip(lower=1).astype(int)
        r_req = (df.RequestedQuantityPacks / df.ForecastDemandPacks.replace(0, np.nan)).fillna(1.1)
        r_app = (df.ApprovedQuantityPacks / df.RequestedQuantityPacks.replace(0, np.nan)).fillna(0.96)
        r_rec = (df.ReceivedQuantityPacks / df.ApprovedQuantityPacks.replace(0, np.nan)).fillna(0.965)
        df["RequestedQuantityPacks"] = (df.ForecastDemandPacks * r_req.clip(0.9, 1.5)).round().astype(int)
        df["ApprovedQuantityPacks"] = (df.RequestedQuantityPacks * r_app.clip(0.8, 1.0)).round().astype(int)
        df["ReceivedQuantityPacks"] = (df.ApprovedQuantityPacks * r_rec.clip(0.85, 1.0)).round().astype(int)
        df["EmergencyRequestQuantityPacks"] = (df.EmergencyRequestQuantityPacks * growth).round().astype(int)
        opening = [int(prev.ClosingStockPacks.get((hh, mm), 0)) for hh, mm in zip(df.HospitalID, df.MedicineID)]
        df["OpeningStockPacks"] = opening
        disp_ratio = (df.DispensedQuantityPacks / df.ConsumedQuantityPacks.replace(0, np.nan)).fillna(1.02)
        want = (cons * disp_ratio.clip(1.0, 1.05)).round().astype(int)
        avail = df.OpeningStockPacks + df.ReceivedQuantityPacks
        waste = df.WastageQuantityPacks.clip(upper=avail)
        dispensed = np.minimum(want, avail - waste)
        df["DispensedQuantityPacks"] = dispensed
        df["ConsumedQuantityPacks"] = np.minimum(cons.astype(int), dispensed)
        df["WastageQuantityPacks"] = waste
        df["ClosingStockPacks"] = avail - dispensed - waste
        df["UnfulfilledDemandPacks"] = (want - dispensed).clip(lower=0)
        df["StockoutDays"] = np.where(df.UnfulfilledDemandPacks > 0,
                                      (df.UnfulfilledDemandPacks / want.clip(lower=1) * self.days).round().clip(1, 8), 0).astype(int)
        df["ServiceLevelPct"] = (100 * (1 - df.UnfulfilledDemandPacks / want.clip(lower=1))).round(2)
        df["MedicationRequestStatus"] = np.where(df.UnfulfilledDemandPacks > 0, "Partially fulfilled", "Completed")
        r0 = max_seq(h.HospitalDemandRecordID)
        df = df.drop(columns="_vf").sort_values(["HospitalID", "MedicineID"]).reset_index(drop=True)
        df["HospitalDemandRecordID"] = [f"HDEM-{r0 + 1 + i:08d}" for i in range(len(df))]
        self.out["06_hospital_demand_data"] = df

    # ------------------------------------------------------------------ 07 IoT
    def gen_iot(self):
        h = self.hist["07_iot_streaming_data"]
        ev = pd.to_datetime(h.EventTimestampUTC, utc=True)
        whz = h[h.AssetType == "WarehouseZone"]
        wev = pd.to_datetime(whz.EventTimestampUTC, utc=True)
        tpl_w = whz[(wev >= wev.max() - pd.Timedelta(days=self.days))]
        tpl_w = tpl_w.assign(_k=tpl_w.groupby("DeviceID").cumcount())
        slots = pd.date_range(self.m_start, self.m_end + pd.Timedelta(hours=21), freq="3h", tz="UTC")
        frames = []
        lastseq = h.groupby("DeviceID").EventSequenceNumber.max()
        for dev, g in tpl_w.groupby("DeviceID"):
            g = g.sort_values("_k")
            k = len(slots)
            rows = g.iloc[np.arange(k) % len(g)].copy()
            rows["EventTimestampUTC"] = slots
            rows["EventSequenceNumber"] = lastseq.get(dev, 0) + 1 + np.arange(k)
            rows["TemperatureC"] = (rows.TemperatureC + self.rng.normal(0, 0.25, k)).round(2)
            rows["RelativeHumidityPct"] = (rows.RelativeHumidityPct + self.rng.normal(0, 0.8, k)).round(2)
            frames.append(rows)
        w = pd.concat(frames, ignore_index=True)
        # one injected cold-room excursion (6h at ~9.6C) to keep the cold-chain story alive for Phase 3
        cold = w.DeviceID.str.contains("COLD")
        if cold.any():
            dev = w[cold].DeviceID.iloc[0]
            idx = w[(w.DeviceID == dev)].index[40:42]
            w.loc[idx, "TemperatureC"] = [9.6, 9.3]
        sh_tpl = h[h.AssetType == "Shipment"]
        frames = []
        dl = self.dl_new.drop_duplicates("ShipmentID")
        hd = self.hist["03_manufacturer_delivery_data"].drop_duplicates("ShipmentID").set_index("ShipmentID")
        by_mode = {mode: g for mode, g in sh_tpl.groupby(sh_tpl.ShipmentID.map(hd.TransportMode))}
        dev0 = max_seq(sh_tpl.DeviceID)
        for i, r in enumerate(dl.itertuples()):
            pool = by_mode.get(r.TransportMode, sh_tpl)
            tid = pool.ShipmentID.drop_duplicates().sample(1, random_state=int(self.rng.integers(1e9))).iloc[0]
            t = pool[pool.ShipmentID == tid].copy()
            t0 = pd.to_datetime(t.EventTimestampUTC, utc=True)
            new0 = pd.Timestamp(r.DispatchDate, tz="UTC")
            t["EventTimestampUTC"] = new0 + (t0 - t0.min())
            t["ShipmentID"] = r.ShipmentID
            t["SSCC18"] = r.SSCC18
            t["DeviceID"] = f"IOT-TRN-{dev0 + 1 + i:07d}"
            t["EventSequenceNumber"] = np.arange(1, len(t) + 1)
            t["TemperatureC"] = (t.TemperatureC + self.rng.normal(0, 0.2, len(t))).round(2)
            frames.append(t)
        s = pd.concat(frames, ignore_index=True) if frames else sh_tpl.head(0)
        df = pd.concat([w, s], ignore_index=True).drop(columns=["_k"], errors="ignore")
        df = df[(df.EventTimestampUTC >= pd.Timestamp(self.m_start, tz="UTC")) |
                (df.AssetType == "Shipment")].copy()
        lat = df.DataLatencySeconds.astype(int)
        df["IngestionTimestampUTC"] = pd.to_datetime(df.EventTimestampUTC, utc=True) + pd.to_timedelta(lat, unit="s")
        df["LateArrivalFlag"] = np.where(lat > 300, "Y", "N")
        df["TemperatureBreachFlag"] = np.where((df.TemperatureC < df.StorageMinTempC) |
                                               (df.TemperatureC > df.StorageMaxTempC), "Y", "N")
        df["HumidityBreachFlag"] = np.where(df.RelativeHumidityPct > df.MaxHumidityPct, "Y", "N")
        shock = df.ShockG > 2.5
        df["ThresholdBreachFlag"] = np.where((df.TemperatureBreachFlag == "Y") | (df.HumidityBreachFlag == "Y") |
                                             shock, "Y", "N")
        df["AlertSeverity"] = np.where(df.TemperatureBreachFlag == "Y", "Critical",
                                       np.where(df.ThresholdBreachFlag == "Y", "Warning", "Normal"))
        df = df.sort_values(["EventTimestampUTC", "DeviceID"]).reset_index(drop=True)
        df["EventTimestampUTC"] = iso_z(df.EventTimestampUTC)
        df["IngestionTimestampUTC"] = iso_z(df.IngestionTimestampUTC)
        r0 = max_seq(h.SensorReadingID)
        df["SensorReadingID"] = [f"IOTR-{r0 + 1 + i:010d}" for i in range(len(df))]
        self.out["07_iot_streaming_data"] = df

    # ------------------------------------------------------------------ dirty rows
    def inject_dirty(self, key):
        df = self.out[key]
        rate = DIRTY_RATE.get(key, 0)
        n = max(1, int(round(len(df) * rate))) if rate else 0
        if n == 0:
            self.dirty[key] = 0
            return
        idx = self.rng.choice(df.index, n, replace=False)
        df = df.astype(object)
        kinds = {"05_pharmacy_sales_data": ["zero_price", "unknown_sku", "null_sku", "bad_date", "amount_mismatch",
                                            "duplicate_pk", "negative_qty"],
                 "06_hospital_demand_data": ["unknown_sku", "null_key", "negative_qty", "bad_date"],
                 "02_manufacturing_data": ["unknown_sku"], "03_manufacturer_delivery_data": ["negative_qty"],
                 "04_warehouse_data": ["unknown_sku"], "07_iot_streaming_data": ["sensor_spike", "null_key"]}[key]
        extra = []
        for j, i in enumerate(idx):
            kind = kinds[j % len(kinds)]
            if kind == "zero_price":
                df.at[i, "UnitRetailPriceUSD"] = 0
            elif kind == "unknown_sku":
                df.at[i, "MedicineID"] = "MED999"
            elif kind == "null_sku":
                df.at[i, "MedicineID"] = None
            elif kind == "bad_date":
                col = "SalesDate" if key.startswith("05") else "DemandMonth"
                df.at[i, col] = f"{self.month}-32"
            elif kind == "amount_mismatch":
                df.at[i, "NetSalesAmountUSD"] = float(df.at[i, "NetSalesAmountUSD"]) + 5
            elif kind == "duplicate_pk":
                extra.append(df.loc[i].copy())
            elif kind == "negative_qty":
                col = {"05_pharmacy_sales_data": "QuantitySoldPacks", "06_hospital_demand_data": "ConsumedQuantityPacks",
                       "03_manufacturer_delivery_data": "ShippedQuantityPacks"}[key]
                df.at[i, col] = -abs(int(df.at[i, col]))
            elif kind == "null_key":
                col = {"06_hospital_demand_data": "HospitalID", "07_iot_streaming_data": "DeviceID"}[key]
                df.at[i, col] = None
            elif kind == "sensor_spike":
                df.at[i, "TemperatureC"] = 999.0
        if extra:
            df = pd.concat([df, pd.DataFrame(extra)], ignore_index=True)
        self.out[key] = df
        self.dirty[key] = n

    # ------------------------------------------------------------------ run + write
    def run(self):
        self.gen_master()
        self.gen_manufacturing()
        self.gen_delivery()
        self.gen_warehouse()
        self.gen_sales()
        self.gen_hospital()
        self.gen_iot()
        out_dir = GENERATED_DIR / self.tag
        (out_dir / "_typed").mkdir(parents=True, exist_ok=True)
        stats = []
        for k in ORDER:
            cols = [c for c, _ in SOURCES[k].columns]
            self.out[k] = self.out[k][cols].reset_index(drop=True)
            clean_copy = self.out[k].copy()
            self.inject_dirty(k)
            df = self.out[k][cols]
            # typed state (clean rows only) for the next refresh's history
            clean_copy.astype({c: str for c in ("GTIN14", "SSCC18") if c in clean_copy}).to_parquet(
                out_dir / "_typed" / f"{k}.parquet", index=False)
            f = out_dir / f"{self.tag}_{k}.xlsx"
            with pd.ExcelWriter(f, engine="xlsxwriter") as xw:
                df.astype(object).where(df.notna(), None).to_excel(xw, sheet_name=k[:31], index=False)
            stats.append(dict(refresh_tag=self.tag, source_name=k, target_month=self.month, rows_generated=len(df),
                              dirty_rows=int(self.dirty.get(k, 0)), output_file=str(f)))
        (out_dir / "generation_stats.json").write_text(json.dumps(stats, indent=2))
        return stats


def publish(tag: str):
    """Hand-off to the pipeline: batch files -> landing/ (file-watcher), sales -> stream_queue/ (streaming API)."""
    import shutil
    src = GENERATED_DIR / tag
    moved = []
    for f in sorted(src.glob(f"{tag}_*.xlsx")):
        target_dir = STREAM_QUEUE_DIR if "05_pharmacy_sales_data" in f.name else LANDING_DIR
        target = target_dir / f.name
        shutil.copy2(f, target.with_suffix(".tmp"))
        target.with_suffix(".tmp").rename(target)   # atomic rename so the watcher never sees a half-written file
        moved.append(str(target))
    return moved


if __name__ == "__main__":
    import sys
    t = sys.argv[1] if len(sys.argv) > 1 else "R1"
    for s in Generator(t).run():
        print(s)
