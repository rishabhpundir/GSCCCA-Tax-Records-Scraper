import argparse
import os
import re
import json
import hashlib
from dataclasses import dataclass, asdict
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple

import cv2
import numpy as np
from PIL import Image
import pytesseract
from multiprocessing import Pool, cpu_count

# ----------------------------
# Config / Patterns
# ----------------------------

CANCEL_WORDS = re.compile(r"\b(CANCELLATION|CANCELLED|FORECLOSURE|FORECLOSED)\b", re.I)

DATE_PATTERNS = [
    # July 14, 2004
    re.compile(r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
               r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+"
               r"(\d{1,2})(?:st|nd|rd|th)?\s*,\s*(\d{4})\b", re.I),
    # 07/14/2004 or 7-14-2004
    re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")
]

AMOUNT_PATTERN = re.compile(r"\$\s*[\d,]+(?:\.\d{2})?")
ZIP_PATTERN = re.compile(r"\bGA\s+(\d{5})(?:-\d{4})?\b", re.I)

# “Filed and Recorded Aug 12, 2004 03:06pm”
FILED_RECORDED_PATTERN = re.compile(
    r"(Filed\s+and\s+Recorded|Filed\s*&\s*Recorded)\s+(.{0,60}?\b\d{4}\b)",
    re.I
)

# Lender-ish phrasing
LENDER_PATTERNS = [
    re.compile(r"\b(?:Lender|Mortgagee)\b\s*[:\-]?\s*(.+)", re.I),
    re.compile(r"\bin\s+favor\s+of\s+(.+?)(?:,|\.)\b", re.I),
    re.compile(r"\bto\s+(.+?)\s*\(\s*\"?Lender\"?\s*\)", re.I),
]

# Borrower/name-ish
NAME_PATTERNS = [
    re.compile(r"\bBorrower(?:s)?\b\s*[:\-]?\s*(.+)", re.I),
    re.compile(r"\bmade\s+this\s+.+?\b,\s*(.+?)\s*\(\s*\"?Borrower", re.I),
    re.compile(r"\b(.*?)\s*\(\s*\"?Borrower", re.I),
]

# Mortgage date-ish (document “made this … day of …” / “dated …”)
MORTGAGE_DATE_PATTERNS = [
    re.compile(r"\bmade\s+this\s+(.{0,40}?\b\d{4}\b)", re.I),
    re.compile(r"\bdated\s+(.{0,40}?\b\d{4}\b)", re.I),
    re.compile(r"\beffective\s+the\s+(.{0,40}?\b\d{4}\b)", re.I),
]

# Address-ish (property located at ...)
ADDRESS_PATTERNS = [
    re.compile(r"\b(property|located\s+at|whose\s+address\s+is)\b.{0,40}?(\d{1,6}\s+.+?\bGA\s+\d{5}(?:-\d{4})?)", re.I),
    re.compile(r"\b(\d{1,6}\s+[A-Z0-9][A-Z0-9\s\.\-#/,]{10,120}\bGA\s+\d{5}(?:-\d{4})?)", re.I),
]

# ----------------------------
# Data model
# ----------------------------

@dataclass
class ExtractedRE:
    file: str
    name: str = ""
    mortgage_date_original: str = ""
    assignment_date: str = ""
    original_lender: str = ""
    mortgage_amount: str = ""
    property_address: str = ""
    cancelled_or_foreclosed: bool = False

# ----------------------------
# Image utils
# ----------------------------

def imread_unicode(path: str) -> np.ndarray:
    """cv2.imread that works with unicode paths on mac/windows."""
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError(f"Failed to read image: {path}")
    return img

def resize_if_needed(img: np.ndarray, target_w: int = 1600) -> np.ndarray:
    h, w = img.shape[:2]
    if w <= target_w:
        return img
    scale = target_w / float(w)
    new_w = target_w
    new_h = int(h * scale)
    return cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)

def preprocess_for_ocr(img: np.ndarray) -> np.ndarray:
    """Fast preprocessing: grayscale + light thresholding."""
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # mild denoise
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    # adaptive threshold (works well for scanned docs)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
                                cv2.THRESH_BINARY, 31, 10)
    return thr

def crop_roi(img: np.ndarray, x1: float, y1: float, x2: float, y2: float) -> np.ndarray:
    """
    ROI crop by relative coordinates (0..1)
    (x1,y1) top-left, (x2,y2) bottom-right
    """
    h, w = img.shape[:2]
    X1 = int(max(0, min(w-1, x1*w)))
    Y1 = int(max(0, min(h-1, y1*h)))
    X2 = int(max(1, min(w,   x2*w)))
    Y2 = int(max(1, min(h,   y2*h)))
    if X2 <= X1 or Y2 <= Y1:
        return img
    return img[Y1:Y2, X1:X2]

def ocr_tesseract(img_bin: np.ndarray, config: str) -> str:
    pil = Image.fromarray(img_bin)
    text = pytesseract.image_to_string(pil, lang="eng", config=config)
    return text.strip()

# ----------------------------
# Parsing helpers
# ----------------------------

def normalize_spaces(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "")).strip()

def find_best_amount(text: str) -> str:
    amounts = AMOUNT_PATTERN.findall(text or "")
    if not amounts:
        return ""
    # choose the largest numeric value
    def amt_value(a: str) -> float:
        a2 = a.replace("$", "").replace(",", "").strip()
        try:
            return float(a2)
        except:
            return 0.0
    best = max(amounts, key=amt_value)
    return normalize_spaces(best)

def find_first_date(text: str) -> str:
    t = text or ""
    # month name formats first
    for m in DATE_PATTERNS[0].finditer(t):
        # return matched span (as-is)
        return normalize_spaces(m.group(0))
    # numeric formats
    for m in DATE_PATTERNS[1].finditer(t):
        return normalize_spaces(m.group(0))
    return ""

def extract_assignment_date(header_text: str) -> str:
    if not header_text:
        return ""
    m = FILED_RECORDED_PATTERN.search(header_text)
    if m:
        # Try to pull first recognizable date inside that fragment
        frag = m.group(0)
        d = find_first_date(frag)
        return d or normalize_spaces(frag)
    # fallback: first date in header
    return find_first_date(header_text)

def extract_mortgage_date(body_text: str) -> str:
    if not body_text:
        return ""
    for pat in MORTGAGE_DATE_PATTERNS:
        m = pat.search(body_text)
        if m:
            candidate = m.group(1)
            d = find_first_date(candidate)
            return d or normalize_spaces(candidate)
    # fallback: first date in body
    return find_first_date(body_text)

def extract_name(body_text: str) -> str:
    t = body_text or ""
    for pat in NAME_PATTERNS:
        m = pat.search(t)
        if m:
            name = m.group(1)
            # clean trailing quotes/parenthesis junk
            name = re.split(r'["\(\)\n\r]', name)[0]
            return normalize_spaces(name)[:120]
    # fallback: look for “Borrowers” nearby
    m = re.search(r"\bBorrower(?:s)?\b.{0,60}", t, re.I)
    if m:
        return normalize_spaces(m.group(0))[:120]
    return ""

def extract_lender(body_text: str) -> str:
    t = body_text or ""
    for pat in LENDER_PATTERNS:
        m = pat.search(t)
        if m:
            lender = m.group(1)
            lender = re.split(r"[\n\r\.]", lender)[0]
            return normalize_spaces(lender)[:140]
    # fallback: “(Lender)” mention
    m = re.search(r"([A-Z][A-Za-z0-9&\-,\. ]{3,120})\s*\(\s*Lender\s*\)", t, re.I)
    if m:
        return normalize_spaces(m.group(1))[:140]
    return ""

def extract_address(body_text: str, fallback_addresses: List[str]) -> str:
    t = body_text or ""
    for pat in ADDRESS_PATTERNS:
        m = pat.search(t)
        if m:
            addr = m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(1)
            return normalize_spaces(addr)[:180]
    # fallback from OCR-json / addresses list
    for a in fallback_addresses or []:
        if ZIP_PATTERN.search(a):
            return normalize_spaces(a)[:180]
    return ""

# ----------------------------
# Optional Paddle (lazy) helper
# ----------------------------

_PADDLE_READY = False
_process_cv2_image = None

def init_paddle_once():
    global _PADDLE_READY, _process_cv2_image
    if _PADDLE_READY:
        return
    try:
        from ocr.ocr_tax_extractor import process_cv2_image  # lazy import
        _process_cv2_image = process_cv2_image
        _PADDLE_READY = True
    except Exception as e:
        _PADDLE_READY = False
        _process_cv2_image = None
        print(f"[WARN] Paddle-based extractor unavailable: {e}")

def try_paddle_addresses_amount(img_bgr: np.ndarray) -> Tuple[List[str], str]:
    """
    Returns (addresses, amount_numeric) best-effort using your existing pipeline.
    Will not run unless --use-paddle is enabled.
    """
    if not _PADDLE_READY or _process_cv2_image is None:
        return [], ""
    try:
        ocr_json = _process_cv2_image(img_bgr)
        addrs = []
        for a in (ocr_json.get("addresses") or []):
            if isinstance(a, str) and a.strip():
                addrs.append(a.strip())
            elif isinstance(a, dict) and a.get("address"):
                addrs.append(str(a["address"]).strip())
        amt = ""
        first_amount = (ocr_json.get("amounts", {}) or {}).get("top_by_score", [{}])
        if first_amount and isinstance(first_amount, list):
            amt = str(first_amount[0].get("numeric") or "").strip()
        return addrs, amt
    except Exception as e:
        print(f"[WARN] Paddle extraction failed: {e}")
        return [], ""

# ----------------------------
# Cache
# ----------------------------

def file_cache_key(path: Path) -> str:
    st = path.stat()
    base = f"{path.resolve()}|{st.st_size}|{int(st.st_mtime)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def load_cache(cache_dir: Path, key: str) -> Optional[Dict]:
    fp = cache_dir / f"{key}.json"
    if fp.exists():
        try:
            return json.loads(fp.read_text(encoding="utf-8"))
        except:
            return None
    return None

def save_cache(cache_dir: Path, key: str, data: Dict) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    fp = cache_dir / f"{key}.json"
    fp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# ----------------------------
# Core extraction per image
# ----------------------------

def extract_from_image(
    img_path: str,
    use_paddle: bool = False,
    cache_dir: Optional[str] = None,
    debug: bool = False,
) -> ExtractedRE:
    p = Path(img_path)
    result = ExtractedRE(file=p.name)

    # Cache
    cache_path = Path(cache_dir) if cache_dir else None
    key = file_cache_key(p)
    if cache_path:
        cached = load_cache(cache_path, key)
        if cached:
            return ExtractedRE(**cached)

    # Read + downscale
    img_bgr = imread_unicode(str(p))
    img_bgr = resize_if_needed(img_bgr, target_w=1600)

    # ROIs (relative coords)
    # Header: top-right area where "Filed and Recorded ..." + date appears
    header_roi = crop_roi(img_bgr, x1=0.55, y1=0.00, x2=1.00, y2=0.18)

    # Body: main paragraph area (avoid giant legal description block if possible)
    body_roi = crop_roi(img_bgr, x1=0.05, y1=0.18, x2=0.95, y2=0.65)

    # Property snippet area often around early body — this is a second, narrower crop
    prop_roi = crop_roi(img_bgr, x1=0.05, y1=0.25, x2=0.95, y2=0.50)

    # Preprocess
    header_bin = preprocess_for_ocr(header_roi)
    body_bin = preprocess_for_ocr(body_roi)
    prop_bin = preprocess_for_ocr(prop_roi)

    # Tesseract configs (fast)
    cfg = "--oem 1 --psm 6"
    header_text = ocr_tesseract(header_bin, cfg)
    body_text = ocr_tesseract(body_bin, cfg)
    prop_text = ocr_tesseract(prop_bin, cfg)

    # Detect cancellation/foreclosure early
    joined = "\n".join([header_text, body_text, prop_text])
    if CANCEL_WORDS.search(joined):
        result.cancelled_or_foreclosed = True
        if cache_path:
            save_cache(cache_path, key, asdict(result))
        return result

    # Optional paddle for better amount/address
    fallback_addrs, paddle_amt = ([], "")
    if use_paddle:
        init_paddle_once()
        fallback_addrs, paddle_amt = try_paddle_addresses_amount(img_bgr)

    # Field extraction
    result.assignment_date = extract_assignment_date(header_text)
    result.name = extract_name(body_text)
    result.original_lender = extract_lender(body_text)

    # Mortgage date often in body first paragraph; include prop_text too
    result.mortgage_date_original = extract_mortgage_date(body_text + "\n" + prop_text)

    # Amount: prefer paddle amount numeric if present; else use biggest $... in text
    if paddle_amt:
        # normalize numeric into $ format if needed
        if paddle_amt and not paddle_amt.startswith("$"):
            result.mortgage_amount = f"${paddle_amt}"
        else:
            result.mortgage_amount = paddle_amt
    else:
        result.mortgage_amount = find_best_amount(joined)

    # Address: prefer address patterns in prop_text/body_text; fallback to paddle addresses
    result.property_address = extract_address(prop_text + "\n" + body_text, fallback_addrs)

    if debug:
        print("\n--- DEBUG (header) ---\n", header_text[:800])
        print("\n--- DEBUG (body) ---\n", body_text[:1200])
        print("\n--- DEBUG (prop) ---\n", prop_text[:800])
        if fallback_addrs:
            print("\n--- DEBUG (paddle addrs) ---\n", fallback_addrs[:3])

    if cache_path:
        save_cache(cache_path, key, asdict(result))
    return result

def extractedre_to_dict(r: ExtractedRE) -> Dict[str, str]:
    """
    Convert OCR result to dict for RealEstateIndexScraper.
    Keys MUST match the Excel headers in realestate_index_scraper.py.
    """
    if r.cancelled_or_foreclosed:
        return {"SKIP_REASON": "CANCELLED/FORECLOSED"}

    return {
        "Name": r.name or "",
        "Mortgage Date (original)": r.mortgage_date_original or "",
        "Assignment Date": r.assignment_date or "",
        "Original Lender": r.original_lender or "",
        "Mortgage Amount": r.mortgage_amount or "",
        "Property Address": r.property_address or "",
        # Optional debug fields you may want in scraper:
        # "cancelled_or_foreclosed": str(bool(r.cancelled_or_foreclosed)),
    }


def extract_re_fields_from_image(
    img_path: str,
    use_paddle: bool = False,
    cache_dir: Optional[str] = None,
    debug: bool = False,
) -> Dict[str, str]:
    """
    Public API for the scraper:
    returns a dictionary ready to data.update(...)

    - If doc is cancelled/foreclosed => returns {"SKIP_REASON": "..."}
    - Else returns your required columns as keys.
    """
    r = extract_from_image(
        img_path=img_path,
        use_paddle=use_paddle,
        cache_dir=cache_dir,
        debug=debug,
    )
    return extractedre_to_dict(r)
# ----------------------------
# IO helpers
# ----------------------------

def gather_images(paths: List[str]) -> List[str]:
    exts = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".webp", ".bmp"}
    out = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            for f in sorted(p.rglob("*")):
                if f.suffix.lower() in exts:
                    out.append(str(f))
        else:
            if p.suffix.lower() in exts:
                out.append(str(p))
    return out

def print_result(r: ExtractedRE):
    print("=" * 80)
    print(r.file)
    if r.cancelled_or_foreclosed:
        print("SKIPPED: Cancelled/Foreclosed detected")
        return
    print(f"Name: {r.name}")
    print(f"Mortgage Date (original): {r.mortgage_date_original}")
    print(f"Assignment Date: {r.assignment_date}")
    print(f"Original Lender: {r.original_lender}")
    print(f"Mortgage Amount: {r.mortgage_amount}")
    print(f"Property Address: {r.property_address}")

def _worker(args):
    return extract_from_image(*args)

# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Fast Real Estate OCR extractor (PNG/JPG). ROI OCR + optional Paddle + cache."
    )
    ap.add_argument("inputs", nargs="+", help="Image file(s) or folder(s)")
    ap.add_argument("--use-paddle", action="store_true",
                    help="Enable optional Paddle-based helper extraction (slower; may init models)")
    ap.add_argument("--workers", type=int, default=1,
                    help="Parallel workers for batches (default 1). Suggest 2-4.")
    ap.add_argument("--cache-dir", type=str, default=".re_ocr_cache",
                    help="Cache directory (default .re_ocr_cache)")
    ap.add_argument("--no-cache", action="store_true", help="Disable caching")
    ap.add_argument("--debug", action="store_true", help="Print debug OCR snippets")
    args = ap.parse_args()

    images = gather_images(args.inputs)
    if not images:
        print("[ERROR] No image files found in inputs.")
        return

    cache_dir = None if args.no_cache else args.cache_dir

    # If using paddle + multiprocessing, safest is workers=1 (paddle init in child can be heavy).
    # Allow it, but warn.
    if args.use_paddle and args.workers > 1:
        print("[WARN] --use-paddle with --workers>1 may be heavy. Consider workers=1.")

    jobs = [(img, args.use_paddle, cache_dir, args.debug) for img in images]

    if args.workers <= 1 or len(images) == 1:
        for j in jobs:
            r = extract_from_image(*j)
            print_result(r)
    else:
        w = min(args.workers, cpu_count())
        with Pool(processes=w) as pool:
            for r in pool.imap_unordered(_worker, jobs):
                print_result(r)

if __name__ == "__main__":
    main()