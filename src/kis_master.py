#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KIS 종목정보파일(.mst) 다운로드/파싱 유틸.

- new.real.download.dws.co.kr 에서 제공하는 kospi/kosdaq/konex master zip을 내려받아 파싱
- 스크리너에서 필요한 최소 필드(티커/종목명/업종코드/기준가/상장주식수)를 안정적으로 제공
- cache_load/cache_save(피클) 기반으로 캐시
"""

from __future__ import annotations

import os
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
import requests

from utils import cache_load, cache_save, CACHE_DIR


_MST_URLS = {
    "KOSPI": ("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip", "kospi_code.mst"),
    "KOSDAQ": ("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip", "kosdaq_code.mst"),
    "KONEX": ("https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip", "konex_code.mst"),
}

_MST_PARSE_VERSION = "v4"
_US_MST_PARSE_VERSION = "v2"
_EXCD_OVRS = {
    "NAS": ("NAS", "NASD"),
    "NYS": ("NYS", "NYSE"),
    "AMS": ("AMS", "AMEX"),
}
_SP500_EXCD_PRIORITY = {"NYS": 0, "NAS": 1, "AMS": 2}

# 해외주식 종목정보파일 (KIS 공식: overseas_stock_code.py)
_OVERSEAS_COD_URLS = {
    "NAS": ("https://new.real.download.dws.co.kr/common/master/nasmst.cod.zip", "nasmst.cod"),
    "NYS": ("https://new.real.download.dws.co.kr/common/master/nysmst.cod.zip", "nysmst.cod"),
    "AMS": ("https://new.real.download.dws.co.kr/common/master/amsmst.cod.zip", "amsmst.cod"),
}
_FRGN_MST_URL = (
    "https://new.real.download.dws.co.kr/common/master/frgn_code.mst.zip",
    "frgn_code.mst",
)

_NASMST_COLUMNS = [
    "National code", "Exchange id", "Exchange code", "Exchange name", "Symbol",
    "realtime symbol", "Korea name", "English name",
    "Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)", "currency",
    "float position", "data type", "base price", "Bid order size", "Ask order size",
    "market start time(HHMM)", "market end time(HHMM)", "DR 여부(Y/N)", "DR 국가코드",
    "업종분류코드", "지수구성종목 존재 여부(0:구성종목없음,1:구성종목있음)",
    "Tick size Type",
    "구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)",
    "Tick size type 상세",
]


def _filter_out_etf_etn_spac_from_mst_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    `.mst` 종목 마스터에서 ETF/ETN/SPAC 원천 제외.

    요구사항:
    - 그룹코드(주식종류)가 일반 주식(ST)인 종목만 남김
    - ETF('EF'), ETN('EN'), 스팩(SPAC) 등은 반드시 제외
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # KOSPI/KOSDAQ 파서는 part2_cols에 "그룹코드"가 존재.
    # KONEX 파서는 최소 컬럼만 확보되므로 "증권그룹구분코드"가 존재할 수 있음.
    group_col_candidates = ["그룹코드", "주식종류", "증권그룹구분코드"]
    group_col = next((c for c in group_col_candidates if c in out.columns), None)

    allow_group_codes = {"ST"}  # 일반 주식만 허용
    if group_col:
        group_code = out[group_col].astype(str).str.strip().str.upper()
        out = out.loc[group_code.isin(allow_group_codes)].copy()

    # KOSPI/KOSDAQ 파싱에는 "SPAC" 컬럼이 존재. (Y/N 또는 코드 형태일 수 있음)
    if "SPAC" in out.columns:
        spac_flag = out["SPAC"].astype(str).str.strip().str.upper()
        mask_spac = spac_flag.isin({"Y", "YES", "1", "SPAC", "TRUE", "T"})
        out = out.loc[~mask_spac].copy()

    return out


def _download_zip(url: str, out_zip_path: Path, timeout_sec: int = 30) -> bool:
    try:
        out_zip_path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, stream=True, timeout=timeout_sec) as r:
            if r.status_code != 200:
                return False
            with open(out_zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)
        return out_zip_path.exists() and out_zip_path.stat().st_size > 0
    except Exception:
        return False


def _extract_file_from_zip(zip_path: Path, inner_name: str, out_dir: Path) -> Optional[Path]:
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(str(zip_path), "r") as zf:
            # zip 내부 파일명이 상이할 수 있어 안전 검색
            names = zf.namelist()
            target = None
            inner_lower = inner_name.lower()
            for n in names:
                base = Path(n).name.lower()
                if base == inner_lower or base.endswith(inner_lower):
                    target = n
                    break
            if target is None and names:
                target = names[0]
            if target is None:
                return None
            zf.extract(target, path=str(out_dir))
            p = out_dir / target
            if p.exists():
                return p
            for cand in out_dir.rglob("*"):
                if cand.is_file() and cand.name.lower().endswith(inner_name.lower().split(".")[-1]):
                    return cand
            return None
    except Exception:
        return None


_extract_mst_from_zip = _extract_file_from_zip


def _parse_nasmst_cod(cod_path: Path) -> pd.DataFrame:
    """나스닥 해외종목 마스터 (탭 구분)."""
    df = pd.read_table(cod_path, sep="\t", encoding="cp949", header=None)
    ncol = min(len(_NASMST_COLUMNS), df.shape[1])
    df = df.iloc[:, :ncol]
    df.columns = _NASMST_COLUMNS[:ncol]
    return df


def _parse_frgn_code_mst(mst_path: Path) -> pd.DataFrame:
    """해외주식지수정보 frgn_code.mst — 나스닥100 편입 플래그 (공식 overseas_index_code.py 이식)."""
    part2_len = 14
    tmp1 = mst_path.with_suffix(".frgn_part1.tmp")
    tmp2 = mst_path.with_suffix(".frgn_part2.tmp")
    with open(tmp1, "w", encoding="cp949") as wf1, open(tmp2, "w", encoding="cp949") as wf2:
        with open(mst_path, "r", encoding="cp949", errors="ignore") as f:
            for row in f:
                row = row.rstrip("\n")
                if len(row) < part2_len + 20:
                    continue
                if row[0:1] == "X":
                    rf1 = row[0 : len(row) - part2_len]
                    rf1_1 = rf1[0:1]
                    rf1_2 = rf1[1:11]
                    rf1_3 = rf1[11:40].replace(",", "")
                    rf1_4 = rf1[40:80].replace(",", "").strip()
                    wf1.write(f"{rf1_1},{rf1_2},{rf1_3},{rf1_4}\n")
                    wf2.write(row[-part2_len:] + "\n")
                    continue
                rf1 = row[0 : len(row) - part2_len]
                rf1_1 = rf1[0:1]
                rf1_2 = rf1[1:11]
                rf1_3 = rf1[11:50].replace(",", "")
                rf1_4 = row[50:75].replace(",", "").strip()
                wf1.write(f"{rf1_1},{rf1_2},{rf1_3},{rf1_4}\n")
                wf2.write(row[-part2_len:] + "\n")
    part1_columns = ["구분코드", "심볼", "영문명", "한글명"]
    df1 = pd.read_csv(str(tmp1), header=None, names=part1_columns, encoding="cp949")
    field_specs = [4, 1, 1, 1, 4, 3]
    part2_columns = [
        "종목업종코드", "다우30 편입종목여부", "나스닥100 편입종목여부",
        "S&P 500 편입종목여부", "거래소코드", "국가구분코드",
    ]
    df2 = pd.read_fwf(str(tmp2), widths=field_specs, names=part2_columns, encoding="cp949")
    for col in ["다우30 편입종목여부", "나스닥100 편입종목여부", "S&P 500 편입종목여부"]:
        if col in df2.columns:
            df2[col] = df2[col].astype(str).str.replace(r"[^0-1]+", "", regex=True)
    df = pd.concat([df1, df2], axis=1)
    try:
        tmp1.unlink(missing_ok=True)
        tmp2.unlink(missing_ok=True)
    except Exception:
        pass
    return df


def _download_overseas_cod(val: str, cache_key: str) -> Optional[Path]:
    val_key = (val or "NAS").upper()
    if val_key not in _OVERSEAS_COD_URLS:
        return None
    url, cod_name = _OVERSEAS_COD_URLS[val_key]
    zip_path = CACHE_DIR / f"{val_key.lower()}mst_{cache_key}.zip"
    out_dir = CACHE_DIR / "kis_master_raw" / cache_key / val_key.lower()
    if not _download_zip(url, zip_path):
        return None
    cod_path = _extract_file_from_zip(zip_path, cod_name, out_dir)
    try:
        zip_path.unlink(missing_ok=True)
    except Exception:
        pass
    return cod_path


def _download_frgn_mst(cache_key: str) -> Optional[Path]:
    url, mst_name = _FRGN_MST_URL
    zip_path = CACHE_DIR / f"frgn_code_{cache_key}.zip"
    out_dir = CACHE_DIR / "kis_master_raw" / cache_key / "frgn"
    if not _download_zip(url, zip_path):
        return None
    mst_path = _extract_file_from_zip(zip_path, mst_name, out_dir)
    try:
        zip_path.unlink(missing_ok=True)
    except Exception:
        pass
    return mst_path


def _standardize_overseas_cod_df(
    df: pd.DataFrame,
    *,
    excd: str,
    ovrs_excg: str,
) -> pd.DataFrame:
    """nasmst/nysmst/amsmst.cod → screener 표준 스키마."""
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    sec_col = "Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)"
    if sec_col in work.columns:
        work = work.loc[work[sec_col].astype(str).str.strip().isin(["2", "2.0"])].copy()
    sym_col = "Symbol" if "Symbol" in work.columns else work.columns[4]
    out = pd.DataFrame()
    out["Code"] = work[sym_col].astype(str).str.strip().str.upper()
    name_col = "English name" if "English name" in work.columns else "Korea name"
    out["Name"] = work.get(name_col, out["Code"]).astype(str)
    if "Korea name" in work.columns:
        kr = work["Korea name"].astype(str).str.strip()
        out["Name"] = out["Name"].where(out["Name"].str.len() > 1, kr)
    out["Close"] = pd.to_numeric(work.get("base price", 0), errors="coerce").fillna(0)
    out["ListedShares"] = 0.0
    sector_col = "업종분류코드"
    if sector_col in work.columns:
        out["Sector"] = work[sector_col].astype(str).str.strip()
        out["Sector"] = out["Sector"].where(out["Sector"].ne("") & out["Sector"].ne("nan"), "N/A")
    else:
        out["Sector"] = "N/A"
    out["SectorSource"] = "mst"
    out["EXCD"] = excd
    out["OvrsExcg"] = ovrs_excg
    out["Marcap"] = out["Close"] * out["ListedShares"]
    out = out[out["Code"].astype(str).str.len() >= 1]
    out = out.drop_duplicates(subset=["Code"]).set_index("Code")
    out.index.name = "Code"
    return out


def _sp500_symbols_from_frgn(frgn: pd.DataFrame) -> set:
    sym = frgn["심볼"].astype(str).str.strip().str.upper()
    sp_flag = frgn.get("S&P 500 편입종목여부", pd.Series(dtype=str)).astype(str).str.strip()
    return set(sym[sp_flag == "1"].tolist())


def _load_sp500_master(cache_key: str, force_refresh: bool = False) -> pd.DataFrame:
    """frgn_code(S&P500=1) ∩ (nasmst + nysmst + amsmst) 주식. 중복 시 NYS > NAS > AMS."""
    cache_id = f"SP500_{cache_key}_{_US_MST_PARSE_VERSION}"
    if not force_refresh:
        cached = cache_load("kis_master", cache_id)
        if isinstance(cached, pd.DataFrame) and not cached.empty:
            return cached

    frgn_path = _download_frgn_mst(cache_key)
    if frgn_path is None:
        return pd.DataFrame()

    try:
        frgn = _parse_frgn_code_mst(frgn_path)
    except Exception:
        return pd.DataFrame()
    if frgn.empty:
        return pd.DataFrame()

    sp500_symbols = _sp500_symbols_from_frgn(frgn)
    if not sp500_symbols:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for val in ("NAS", "NYS", "AMS"):
        excd, ovrs = _EXCD_OVRS[val]
        cod_path = _download_overseas_cod(val, cache_key)
        if cod_path is None:
            continue
        try:
            raw = _parse_nasmst_cod(cod_path)
            std = _standardize_overseas_cod_df(raw, excd=excd, ovrs_excg=ovrs)
        except Exception:
            continue
        if std.empty:
            continue
        hit = std[std.index.isin(sp500_symbols)].copy()
        if not hit.empty:
            frames.append(hit)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames)
    out["_pri"] = out["EXCD"].map(_SP500_EXCD_PRIORITY).fillna(9)
    out = out.sort_values("_pri")
    out = out[~out.index.duplicated(keep="first")]
    out = out.drop(columns=["_pri"])

    cache_save("kis_master", cache_id, out)
    return out


def _parse_kospi_kosdaq_mst(mst_path: Path, market: str) -> pd.DataFrame:
    """
    공식 샘플(kis_kospi_code_mst.py / kis_kosdaq_code_mst.py) 파싱 로직을 최소화하여 이식.
    """
    market = market.upper()
    if market not in ("KOSPI", "KOSDAQ"):
        raise ValueError("market must be KOSPI or KOSDAQ")

    # part2 길이:
    # - _parse_kospi_kosdaq_mst 내부의 widths 합과 맞춰야 합니다.
    # - 현재 kis_master.py의 KOSPI widths 합이 227이라서 part2_len도 227로 맞춥니다.
    part2_len = 227 if market == "KOSPI" else 222
    tmp1 = mst_path.with_suffix(".part1.tmp")
    tmp2 = mst_path.with_suffix(".part2.tmp")

    with open(tmp1, "w", encoding="cp949") as wf1, open(tmp2, "w", encoding="cp949") as wf2:
        with open(mst_path, "r", encoding="cp949", errors="ignore") as f:
            for row in f:
                row = row.rstrip("\n")
                if len(row) < part2_len + 21:
                    continue
                rf1 = row[0 : len(row) - part2_len]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
                wf2.write(row[-part2_len:] + "\n")

    if market == "KOSPI":
        part1_cols = ["단축코드", "표준코드", "한글명"]
        widths = [
            2, 1, 4, 4, 4,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 9, 5, 5, 1,
            1, 1, 2, 1, 1,
            1, 2, 2, 2, 3,
            1, 3, 12, 12, 8,
            15, 21, 2, 7, 1,
            1, 1, 1, 1, 9,
            9, 9, 5, 9, 8,
            9, 3, 1, 1, 1,
        ]
        part2_cols = [
            "그룹코드", "시가총액규모", "지수업종대분류", "지수업종중분류", "지수업종소분류",
            "제조업", "저유동성", "지배구조지수종목", "KOSPI200섹터업종", "KOSPI100",
            "KOSPI50", "KRX", "ETP", "ELW발행", "KRX100",
            "KRX자동차", "KRX반도체", "KRX바이오", "KRX은행", "SPAC",
            "KRX에너지화학", "KRX철강", "단기과열", "KRX미디어통신", "KRX건설",
            "Non1", "KRX증권", "KRX선박", "KRX섹터_보험", "KRX섹터_운송",
            "SRI", "기준가", "매매수량단위", "시간외수량단위", "거래정지",
            "정리매매", "관리종목", "시장경고", "경고예고", "불성실공시",
            "우회상장", "락구분", "액면변경", "증자구분", "증거금비율",
            "신용가능", "신용기간", "전일거래량", "액면가", "상장일자",
            "상장주수", "자본금", "결산월", "공모가", "우선주",
            "공매도과열", "이상급등", "KRX300", "KOSPI", "매출액",
            "영업이익", "경상이익", "당기순이익", "ROE", "기준년월",
            "시가총액", "그룹사코드", "회사신용한도초과", "담보대출가능", "대주가능",
        ]
    else:
        part1_cols = ["단축코드", "표준코드", "한글종목명"]
        widths = [
            2, 1,
            4, 4, 4, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 9,
            5, 5, 1, 1, 1,
            2, 1, 1, 1, 2,
            2, 2, 3, 1, 3,
            12, 12, 8, 15, 21,
            2, 7, 1, 1, 1,
            1, 9, 9, 9, 5,
            9, 8, 9, 3, 1,
            1, 1, 1,
        ]
        part2_cols = [
            "증권그룹구분코드", "시가총액 규모 구분 코드 유가",
            "지수업종 대분류 코드", "지수 업종 중분류 코드", "지수업종 소분류 코드", "벤처기업 여부 (Y/N)",
            "저유동성종목 여부", "KRX 종목 여부", "ETP 상품구분코드", "KRX100 종목 여부 (Y/N)",
            "KRX 자동차 여부", "KRX 반도체 여부", "KRX 바이오 여부", "KRX 은행 여부", "기업인수목적회사여부",
            "KRX 에너지 화학 여부", "KRX 철강 여부", "단기과열종목구분코드", "KRX 미디어 통신 여부",
            "KRX 건설 여부", "(코스닥)투자주의환기종목여부", "KRX 증권 구분", "KRX 선박 구분",
            "KRX섹터지수 보험여부", "KRX섹터지수 운송여부", "KOSDAQ150지수여부 (Y,N)", "주식 기준가",
            "정규 시장 매매 수량 단위", "시간외 시장 매매 수량 단위", "거래정지 여부", "정리매매 여부",
            "관리 종목 여부", "시장 경고 구분 코드", "시장 경고위험 예고 여부", "불성실 공시 여부",
            "우회 상장 여부", "락구분 코드", "액면가 변경 구분 코드", "증자 구분 코드", "증거금 비율",
            "신용주문 가능 여부", "신용기간", "전일 거래량", "주식 액면가", "주식 상장 일자", "상장 주수(천)",
            "자본금", "결산 월", "공모 가격", "우선주 구분 코드", "공매도과열종목여부", "이상급등종목여부",
            "KRX300 종목 여부 (Y/N)", "매출액", "영업이익", "경상이익", "단기순이익", "ROE(자기자본이익률)",
            "기준년월", "전일기준 시가총액 (억)", "그룹사 코드", "회사신용한도초과여부", "담보대출가능여부", "대주가능여부",
        ]

    df1 = pd.read_csv(str(tmp1), header=None, names=part1_cols, encoding="cp949")
    df2 = pd.read_fwf(str(tmp2), widths=widths, names=part2_cols, encoding="cp949")
    df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)

    try:
        tmp1.unlink(missing_ok=True)
        tmp2.unlink(missing_ok=True)
    except Exception:
        pass
    return df


def _parse_konex_mst(mst_path: Path) -> pd.DataFrame:
    # 공식 샘플(kis_konex_code_mst.py)와 동일하게 슬라이싱 파싱
    rows: List[List[str]] = []
    with open(mst_path, "r", encoding="cp949", errors="ignore") as f:
        for row in f:
            row = row.rstrip("\n")
            if len(row) < 21 + 184:
                continue
            mksc_shrn_iscd = row[0:9].strip()
            stnd_iscd = row[9:21].strip()
            hts_kor_isnm = row[21:-184].strip()
            scrt_grp_cls_code = row[-184:-182].strip()
            stck_sdpr = row[-182:-173].strip()
            lstn_stcn = row[-110:-95].strip()
            # 업종코드가 없어서 최소 필드만 확보
            rows.append([mksc_shrn_iscd, stnd_iscd, hts_kor_isnm, scrt_grp_cls_code, stck_sdpr, lstn_stcn])
    return pd.DataFrame(rows, columns=["단축코드", "표준코드", "종목명", "증권그룹구분코드", "주식 기준가", "상장 주수(천)"])


def load_kis_master(market: str, cache_key: Optional[str] = None, force_refresh: bool = False) -> pd.DataFrame:
    """
    KOSPI/KOSDAQ/KONEX 종목정보(.mst) 로드.
    - cache_key: 보통 date_str(YYYYMMDD) 권장
    - 반환 DF: 최소한 단축코드/한글명(또는 종목명)/기준가/상장주수(또는 상장 주수(천)) 및 업종코드 컬럼(있으면)
    """
    market = (market or "").upper()
    ck = cache_key or datetime_now_yyyymmdd()

    if market in ("SP500", "SPX500", "S&P500"):
        return _load_sp500_master(ck, force_refresh=force_refresh)
    if market in ("NASDAQ100", "NDX100"):
        import logging
        logging.getLogger(__name__).warning(
            "NASDAQ100 is deprecated; loading SP500 universe instead."
        )
        return _load_sp500_master(ck, force_refresh=force_refresh)

    cache_id = f"{market}_{ck}_{_MST_PARSE_VERSION}"
    cached = None if force_refresh else cache_load("kis_master", cache_id)
    if isinstance(cached, pd.DataFrame) and not cached.empty:
        # (중요) 캐시가 이전 파싱 로직에서 생성된 경우 7~8자리 Code가 섞일 수 있으므로
        # 여기서도 6자리만 다시 강제 필터링합니다.
        try:
            idx = cached.index.astype(str)
            stripped = idx.str.replace(r"[^0-9]", "", regex=True)
            # pandas.Index 타입에서는 .eq()가 없을 수 있어 == 로 비교
            mask = stripped.str.len() == 6
            if mask.all():
                return cached
            return cached.loc[mask].copy()
        except Exception:
            # 캐시 형태가 예상과 달라도 파이프라인이 멈추지 않게 방어
            return cached

    if market not in _MST_URLS:
        return pd.DataFrame()
    url, mst_name = _MST_URLS[market]

    zip_path = CACHE_DIR / f"{market.lower()}_code_{ck}.zip"
    out_dir = CACHE_DIR / "kis_master_raw" / ck
    if not _download_zip(url, zip_path):
        return pd.DataFrame()
    mst_path = _extract_mst_from_zip(zip_path, mst_name, out_dir)
    try:
        zip_path.unlink(missing_ok=True)
    except Exception:
        pass
    if mst_path is None or not mst_path.exists():
        return pd.DataFrame()

    try:
        if market in ("KOSPI", "KOSDAQ"):
            df = _parse_kospi_kosdaq_mst(mst_path, market)
        else:
            df = _parse_konex_mst(mst_path)
    except Exception:
        return pd.DataFrame()

    # 종목 원천 타입 필터(ETF/ETN/SPAC 제외)
    df = _filter_out_etf_etn_spac_from_mst_df(df)

    # 표준화: ticker/name/price/shares/sector_code
    out = pd.DataFrame()
    # ticker
    for c in ["단축코드", "mksc_shrn_iscd"]:
        if c in df.columns:
            out["Code"] = df[c].astype(str).str.replace(r"[^0-9]", "", regex=True).str.zfill(6)
            break
    # name
    for c in ["한글명", "한글종목명", "종목명", "hts_kor_isnm"]:
        if c in df.columns:
            out["Name"] = df[c].astype(str)
            break
    # base price
    for c in ["기준가", "주식 기준가", "stck_sdpr"]:
        if c in df.columns:
            out["Close"] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            break
    # shares
    shares_col = None
    for c in ["상장주수", "상장 주수(천)", "lstn_stcn"]:
        if c in df.columns:
            shares_col = c
            break
    if shares_col:
        s = pd.to_numeric(df[shares_col], errors="coerce").fillna(0)
        # KIS MST의 상장주수 단위가 문서/샘플 기준으로 "백 주(=100주)" 단위로 제공되는 경우가 있어,
        # 최소한 KOSPI의 "상장주수"는 100배 보정이 필요합니다. (삼성전자: MST 상장주수 5,919,637 -> 약 591,963,700주)
        if shares_col == "상장주수":
            s = s * 100
        elif "천" in shares_col:
            # KOSDAQ의 경우 일반적으로 "천 주" 단위 컬럼을 사용합니다.
            s = s * 1000
        out["ListedShares"] = s
    else:
        out["ListedShares"] = 0
    # sector codes (가능한 경우)
    sector_cols = []
    for c in ["지수업종대분류", "지수업종중분류", "지수업종소분류", "지수업종 대분류 코드", "지수 업종 중분류 코드", "지수업종 소분류 코드"]:
        if c in df.columns:
            sector_cols.append(c)
    if sector_cols:
        # 3개가 모두 있을 때만 조합
        vals = []
        for c in sector_cols[:3]:
            vals.append(df[c].astype(str).str.strip())
        if len(vals) == 3:
            out["Sector"] = ("IDX_" + vals[0] + "-" + vals[1] + "-" + vals[2]).where(vals[0].ne("") & vals[0].ne("nan"), "N/A")
        else:
            out["Sector"] = "N/A"
    else:
        out["Sector"] = "N/A"
    out["SectorSource"] = out["Sector"].apply(lambda x: "mst" if str(x).strip() and str(x).strip().upper() not in {"N/A", "NA", "NAN"} else "unknown")
    out["Marcap"] = (pd.to_numeric(out.get("Close", 0), errors="coerce").fillna(0) * pd.to_numeric(out.get("ListedShares", 0), errors="coerce").fillna(0)).astype("float64")

    out = out.dropna(subset=["Code"])
    # KRX 종목 코드는 반드시 6자리여야 합니다.
    # (mst 파싱 과정/데이터 품질 이슈로 7자리 이상이 섞일 수 있어 테스트 및 조인 안정성을 위해 강제)
    out = out[out["Code"].astype(str).str.len().eq(6)]
    out = out.set_index("Code")
    out.index.name = "Code"

    cache_save("kis_master", cache_id, out)
    return out


def datetime_now_yyyymmdd() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d")

