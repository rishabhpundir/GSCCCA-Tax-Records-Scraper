#!/usr/bin/env python3
"""
OCR extractor for Georgia tax / FIFA / court documents from vertical PNG images.

- Input: one or more PNG/JPEG/TIFF images (already in correct vertical orientation).
- Output: JSON with dollar amounts and address blocks.

Usage:
    python ocr_tax_extractor.py path/to/image1.png path/to/folder --pretty
"""

import os
os.environ["DISABLE_MODEL_SOURCE_CHECK"] = "True"

import re
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import cv2
import difflib
import pytesseract
import numpy as np
from PIL import Image
from pytesseract import Output
from functools import lru_cache
from paddleocr import PaddleOCR
from rich.console import Console


# ----------------- SETUP ----------------- #
console = Console()

# load Tesseract path for Windows if needed
try:
    if os.name == "nt":  # Windows
        pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
except Exception as e:
    console.print(f"[red]Error setting up Tesseract: {e}[/red]")


# ----------------- REGEXES & CONSTANTS ----------------- #
MONEY_RE = re.compile(r"\$\s*[\d,]+(?:\.\d{1,2})?")
ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")
US_STATE_ABBRS = ["GA", "FL"]

ROI_TESS_PSMS = (6, 11, 12)
TOTAL_DECIMAL_RE = re.compile(r"(?is)\bTOTAL\b.{0,80}[\d,]+\.\d{2}")
DECIMAL_RE = re.compile(r"[\d,]+\.\d{2}")
DESCRIPTION_RE = re.compile(r"(?i)\bDESCRIPTION\b")
USE_SLOW_DENOISE = False

STATE_ZIP_RE = re.compile(
    rf"\b(?:{'|'.join(US_STATE_ABBRS)})\b\s*,?\s*\d{{5}}(?:-\d{{4}})?\b",
    re.IGNORECASE,
)
    
# =========================
# ADD (near your other regex/constants, anywhere above extract_address_blocks)
# =========================
ADDR_SUFFIXES = (
    "ST", "AVE", "RD", "DR", "CT", "LN", "BLVD", "HWY", "WAY", "PL", "PKWY", "CIR",
    "TER", "TRL", "SQ", "PT", "RUN", "CV", "CVS", "BND", "XING", "HOLW", "HL", "HLS",
    "PARK", "VW", "VIEW", "PASS", "WALK", "TRCE", "TRAK", "PATH", "PIKE", "RTE",
    "STE", "SUITE", "UNIT", "APT", "FL"
)

# Start anchor for an address block (either PO BOX or a street number + street suffix)
ADDR_START_RE = re.compile(
    rf"(?ix)\b("
    rf"P\.?\s*O\.?\s*BOX\s*\d+"
    rf"|"
    rf"\d{{1,6}}\s+[A-Z0-9][A-Z0-9\s\.]{{1,80}}?\b(?:{'|'.join(ADDR_SUFFIXES)})\b\.?"
    rf")"
)

ADDR_GARBAGE_RE = re.compile(
    r"(?ix)\b("
    r"levy|"
    r"fifa|"
    r"fi\.?\s*fa\.?|"
    r"ley|"
    r"py|"
    r"tfa"
    r")\b"
)


#----------------- OCR HELPERS ----------------- #
@lru_cache(maxsize=1)
def get_paddle_ocr():
    return PaddleOCR(
        lang="en",
        use_textline_orientation=False,
    )
    

def _trim_to_address_span(cleaned: str, state_zip_re: re.Pattern) -> str:
    """
    Keep only the address-like portion ending at STATE+ZIP by trimming leading junk.
    Example:
      'PaymentstoDate ... 86 FOLIAGE CT Levy ... DALLAS GA 30132'
        -> '86 FOLIAGE CT, DALLAS GA 30132'
    """
    m = state_zip_re.search(cleaned)
    if not m:
        return cleaned.strip(" ,")

    s = cleaned[: m.end()]  # already your intended termination point

    # Prefer last PO BOX / street-address start within the truncated span
    last = None
    for mm in ADDR_START_RE.finditer(s):
        last = mm
    if last:
        s = s[last.start() :]
    else:
        # fallback: last street-number-like token before STATE+ZIP
        prefix = s[: m.start()]
        nums = list(re.finditer(r"\b\d{1,6}\b", prefix))
        if nums:
            s = s[nums[-1].start() :]

    # remove embedded OCR garbage tokens like "Levy", "Fi.Fa.", "PY", "tfa" etc.
    s = ADDR_GARBAGE_RE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"(, ){2,}", ", ", s).strip(" ,")
    return s

    
# ✅ ADD these helpers BELOW preprocess_image() (and ABOVE ocr_image()/ocr_data())
def _to_bgr(img: np.ndarray) -> np.ndarray:
    return img if (img is not None and img.ndim == 3) else cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)


def _remove_table_lines(gray: np.ndarray) -> np.ndarray:
    """Remove horizontal/vertical ruling lines (tables) to improve OCR recall for numbers."""
    if gray.ndim != 2:
        gray = cv2.cvtColor(gray, cv2.COLOR_BGR2GRAY)

    inv = cv2.bitwise_not(gray)

    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (60, 1))
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 60))

    horiz = cv2.morphologyEx(inv, cv2.MORPH_OPEN, h_kernel, iterations=1)
    vert = cv2.morphologyEx(inv, cv2.MORPH_OPEN, v_kernel, iterations=1)
    lines = cv2.bitwise_or(horiz, vert)

    inv_clean = cv2.bitwise_and(inv, cv2.bitwise_not(lines))
    return cv2.bitwise_not(inv_clean)


# ✅ ADD these helpers BELOW _remove_table_lines() (and above ensemble_ocr / process_cv2_image)
def _table_roi(img_bgr: np.ndarray) -> np.ndarray:
    h, w = img_bgr.shape[:2]
    x0 = int(w * 0.38)
    x1 = int(w * 0.98)
    y0 = int(h * 0.18)
    y1 = int(h * 0.62)
    return img_bgr[y0:y1, x0:x1].copy()


def _roi_variants(roi_bgr: np.ndarray) -> List[np.ndarray]:
    gray = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)
    otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    inv_otsu = cv2.bitwise_not(otsu)
    line_removed = _remove_table_lines(gray)
    up2 = cv2.resize(gray, (0, 0), fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    up2_otsu = cv2.threshold(up2, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    up2_lr = _remove_table_lines(up2)
    return [gray, otsu, inv_otsu, line_removed, up2_otsu, up2_lr]


def _recover_table_text(img_bgr: np.ndarray, *, want_description: bool = False) -> str:
    roi = _table_roi(img_bgr)
    out_lines: List[str] = []
    seen = set()

    def _keep_line(s: str) -> bool:
        return (
            ("TOTAL" in s.upper())
            or (DECIMAL_RE.search(s) is not None)
            or (MONEY_RE.search(s) is not None)
            or (want_description and (DESCRIPTION_RE.search(s) is not None))
        )

    # Tesseract passes on ROI only (fast)
    for vimg in _roi_variants(roi):
        for psm in ROI_TESS_PSMS:
            txt = _tess_text(vimg, psm)
            for ln in txt.splitlines():
                s = ln.strip()
                if not s:
                    continue
                if not _keep_line(s):
                    continue
                key = re.sub(r"\s+", " ", s).strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                out_lines.append(s)

    joined = "\n".join(out_lines)

    # If ROI Tesseract already recovered what we need, don't pay Paddle cost
    if (TOTAL_DECIMAL_RE.search(joined) is not None) or (want_description and (DESCRIPTION_RE.search(joined) is not None)):
        return joined.strip()

    # Paddle pass on ROI only (and ROI-up2) — only if needed
    roi_up2 = cv2.resize(roi, (0, 0), fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    for s in (_paddle_lines(roi) + _paddle_lines(roi_up2)):
        if not _keep_line(s):
            continue
        key = re.sub(r"\s+", " ", s).strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out_lines.append(s)

    return "\n".join(out_lines).strip()


def _tess_cfg(psm: int) -> str:
    # keep psm per pass, maximize recall, avoid dictionary "corrections"
    return (
        f"--oem 3 --psm {psm} "
        f"-c preserve_interword_spaces=1 "
        f"-c load_system_dawg=0 -c load_freq_dawg=0"
    )


def _tess_text(img: np.ndarray, psm: int) -> str:
    pil_img = cv_to_pil(_to_bgr(img))
    return pytesseract.image_to_string(pil_img, config=_tess_cfg(psm))


def _paddle_lines(img_bgr: np.ndarray) -> List[str]:
    """Robustly extract text lines from PaddleOCR across different output formats."""
    ocr = get_paddle_ocr()
    try:
        # newer PaddleOCR prefers predict()
        res = ocr.predict(img_bgr)
    except Exception:
        res = ocr.ocr(img_bgr)

    lines: List[str] = []
    if not res:
        return lines

    # Newer pipeline format: [ { rec_texts: [...], rec_scores: [...], ... } ]
    if isinstance(res, list) and isinstance(res[0], dict):
        texts = res[0].get("rec_texts") or []
        for t in texts:
            s = str(t).strip()
            if s:
                lines.append(s)
        return lines

    # Classic format: [[ [box, (text, conf)], ... ]]
    payload = res[0] if (isinstance(res, list) and len(res) == 1 and isinstance(res[0], list)) else res
    if isinstance(payload, list):
        for item in payload:
            if not item:
                continue
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                rec = item[1]
                if isinstance(rec, (list, tuple)) and len(rec) >= 1:
                    s = str(rec[0]).strip()
                else:
                    s = str(rec).strip()
                if s:
                    lines.append(s)
    return lines

def ensemble_ocr(
    img_bgr: np.ndarray,
    *,
    preprocessed: Optional[np.ndarray] = None,
    preprocessed_data: Optional[np.ndarray] = None,
) -> Tuple[str, Dict[str, Any]]:
    pre = preprocessed if preprocessed is not None else preprocess_image(img_bgr)
    base_text = ocr_image(pre)
    base_data_img = preprocessed_data if preprocessed_data is not None else pre
    base_data = ocr_data(base_data_img)

    # Only do expensive stuff if TOTAL+decimal or DESCRIPTION is missing
    missing_total = (TOTAL_DECIMAL_RE.search(base_text) is None)
    missing_desc = (DESCRIPTION_RE.search(base_text) is None)

    if missing_total or missing_desc:
        extra = _recover_table_text(img_bgr, want_description=missing_desc)
        if extra:
            base_text = base_text + "\n" + extra

    return base_text, base_data


def ocr_data(img: np.ndarray):
    pil_img = cv_to_pil(img)
    return pytesseract.image_to_data(pil_img, config="--oem 3 --psm 6", output_type=Output.DICT)


def data_to_lines(data):
    n = len(data.get("text", []))
    items = []
    for i in range(n):
        txt = str(data["text"][i]).strip()
        if not txt:
            continue
        items.append({
            "text": txt,
            "left": int(data["left"][i]),
            "top": int(data["top"][i]),
            "width": int(data["width"][i]),
            "height": int(data["height"][i]),
            "block": int(data["block_num"][i]),
            "par": int(data["par_num"][i]),
            "line": int(data["line_num"][i]),
        })

    groups = {}
    for it in items:
        groups.setdefault((it["block"], it["par"], it["line"]), []).append(it)

    lines = []
    for _, words in groups.items():
        words = sorted(words, key=lambda w: w["left"])
        text = " ".join(w["text"] for w in words)
        l = min(w["left"] for w in words)
        t = min(w["top"] for w in words)
        r = max(w["left"] + w["width"] for w in words)
        b = max(w["top"] + w["height"] for w in words)
        lines.append({"text": text, "bbox": (l, t, r, b)})

    lines.sort(key=lambda x: x["bbox"][1])
    return lines


# ----------------- Main ----------------- #
def pil_to_cv(img: Image.Image) -> np.ndarray:
    """Convert a PIL image to an OpenCV BGR numpy array."""
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)


def cv_to_pil(img: np.ndarray) -> Image.Image:
    """Convert OpenCV BGR image to PIL."""
    return Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))


def preprocess_image(img: np.ndarray, *, upscale: float = 2.0) -> np.ndarray:
    if img is None:
        raise ValueError("input image is None (cv2.imread failed)")

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Upscale helps a LOT for small fonts (your scan is ~1000px wide)
    if upscale and upscale != 1.0:
        gray = cv2.resize(gray, (0, 0), fx=upscale, fy=upscale, interpolation=cv2.INTER_CUBIC)

    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    gray = clahe.apply(gray)

    if USE_SLOW_DENOISE:
        gray = cv2.fastNlMeansDenoising(gray, None, h=15, templateWindowSize=7, searchWindowSize=21)
    else:
        gray = cv2.medianBlur(gray, 3)

    # Mild sharpening
    blur = cv2.GaussianBlur(gray, (0, 0), sigmaX=1.2)
    sharp = cv2.addWeighted(gray, 1.5, blur, -0.5, 0)

    _, thresh = cv2.threshold(sharp, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return thresh


def ocr_image(img: np.ndarray) -> str:
    """
    Run Tesseract OCR on a preprocessed image and return plain text.
    Assumes image is already correctly oriented (vertical).
    """
    pil_img = cv_to_pil(img)
    text = pytesseract.image_to_string(pil_img, config=_tess_cfg(6))
    return text


def ocr_data(img: np.ndarray) -> Dict[str, Any]:
    """Tesseract OCR that returns word-level data with bounding boxes."""
    pil_img = cv_to_pil(img)
    return pytesseract.image_to_data(
        pil_img,
        config=_tess_cfg(6),
        output_type=Output.DICT,
    )


def data_to_lines(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Group Tesseract word-level data into line-level entries with a union bbox."""
    n = len(data.get("text", []))
    items = []
    for i in range(n):
        txt = str(data["text"][i]).strip()
        if not txt:
            continue
        items.append({
            "text": txt,
            "left": int(data["left"][i]),
            "top": int(data["top"][i]),
            "width": int(data["width"][i]),
            "height": int(data["height"][i]),
            "block": int(data.get("block_num", [0]*n)[i]),
            "par": int(data.get("par_num", [0]*n)[i]),
            "line": int(data.get("line_num", [0]*n)[i]),
        })

    groups: Dict[tuple, List[dict]] = {}
    for it in items:
        groups.setdefault((it["block"], it["par"], it["line"]), []).append(it)

    lines: List[Dict[str, Any]] = []
    for words in groups.values():
        words = sorted(words, key=lambda w: w["left"])
        text = " ".join(w["text"] for w in words)
        l = min(w["left"] for w in words)
        t = min(w["top"] for w in words)
        r = max(w["left"] + w["width"] for w in words)
        b = max(w["top"] + w["height"] for w in words)
        lines.append({"text": text, "bbox": (l, t, r, b)})

    lines.sort(key=lambda x: x["bbox"][1])
    return lines


# ----------------- TEXT PARSING HELPERS ----------------- #
def extract_amounts(text: str) -> Dict[str, Any]:
    """
    Extract dollar amounts from OCR text.

    Returns:
        {
            "candidates": [
                {"raw": "$251.67", "numeric": 251.67, "line": "...", "score": 12.251},
                ...
            ],
            "top_by_score": [ ... up to 3 best ... ]
        }
    """
    candidates = []

    importance_keywords = {
        "TOTAL DUE": 12,
        "TOTAL LIEN": 10,
        "TOTAL AMOUNT": 10,
        "TOTAL": 10,
        "BALANCE DUE": 10,
        "BALANCE": 6,
        "PAID AMOUNT": 8,
        "PAID": 4,
        "DUE": 4,
        "TAX": 2,
    }

    for line in text.splitlines():
        raw_line = line.strip()
        if not raw_line:
            continue

        upper = raw_line.upper()
        norm_line = raw_line.replace("§", "$")
        norm_line = re.sub(r"\bS\s*(?=\d)", "$", norm_line)

        for match in MONEY_RE.finditer(norm_line):
            raw = match.group().replace(" ", "")
            num_str = raw.replace("$", "").replace(",", "")

            try:
                value = float(num_str)
            except ValueError:
                value = None

            score = 0.0
            for kw, weight in importance_keywords.items():
                if kw in upper:
                    score += weight

            # small bias toward higher amounts
            if value is not None:
                score += value / 1000.0

            candidates.append(
                {
                    "raw": raw,
                    "numeric": value,
                    "line": raw_line,
                    "score": round(score, 3),
                }
            )
            
    # ----------------- FALLBACK (no "$" >= 100 found) -----------------
    has_big_dollar = any(
        (c.get("numeric") is not None)
        and (c.get("raw", "").lstrip().startswith("$"))
        and (float(c["numeric"]) >= 100.0)
        for c in candidates
    )

    if not has_big_dollar:
        for line in text.splitlines():
            raw_line = line.strip()
            if not raw_line:
                continue
            upper = raw_line.upper()
            if "TOTAL" not in upper:
                continue

            nums = []
            for m in DECIMAL_RE.finditer(raw_line):
                s = m.group(0)
                try:
                    v = float(s.replace(",", ""))
                except ValueError:
                    continue
                nums.append((v, s))
            if not nums:
                continue

            value, raw_num = max(nums, key=lambda x: x[0])

            score = 0.0
            for kw, weight in importance_keywords.items():
                if kw in upper:
                    score += weight
            score += 5.0  # boost for TOTAL-without-$ recovery
            score += value / 1000.0

            candidates.append(
                {
                    "raw": raw_num,
                    "numeric": value,
                    "line": raw_line,
                    "score": round(score, 3),
                }
            )

    candidates_sorted = sorted(
        candidates,
        key=lambda c: (c["score"] if c["score"] is not None else float("-inf")),
        reverse=True,
    )
    top = candidates_sorted[:3] if len(candidates_sorted) >= 4 else candidates_sorted
    return {
        "top_by_score": top,
        # "candidates": candidates_sorted,
    }


def extract_description(text: str) -> Optional[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    REJECT_WORDS = {
        "fee", "fees", "total", "tax", "taxes",
        "lien", "interest", "penalty", "cost"
    }
    WORDS_TO_CHECK = ("MEFF", "INVENT", "BOAT", "INVENTI", "EQUIPMENT")

    # Compute once for the whole OCR text
    full_text_lower = text.lower()
    matched_words = []
    for w in WORDS_TO_CHECK:
        if w.lower() in full_text_lower:
            matched_words.append(w)
    matched_words = list(dict.fromkeys(matched_words))  # de-dupe, preserve order

    def clean_description(val: str) -> Optional[str]:
        if not val:
            return None

        # Cut trailing OCR junk after wide gaps
        val = re.split(r"\s{3,}", val)[0]

        # Remove non-alphanumeric chars except spaces
        val = re.sub(r"[^A-Za-z0-9 ]+", " ", val)

        # Normalize spaces
        val = re.sub(r"\s+", " ", val).strip()

        # Must contain at least one letter
        if not re.search(r"[A-Za-z]", val):
            return None

        # Reject pure numbers / decimals / money
        if re.fullmatch(r"\$?\d+(\.\d+)?", val):
            return None

        # Reject unwanted domain words
        words = {w.lower() for w in val.split()}
        if words & REJECT_WORDS:
            return None

        return val

    for i, line in enumerate(lines):
        if re.search(r"(?i)\b(property\s+description|property\s+location|description)\b", line):
            # Remove everything up to and including "description"
            remainder = re.sub(r"(?i).*?\b(property\s+description|property\s+location|description)\b", "", line)

            # Remove common OCR separators after Description
            remainder = re.sub(r"^[\s:=\-|§«=]+", "", remainder).strip()
            if not remainder and i + 1 < len(lines):
                remainder = lines[i + 1].strip()

            cleaned = clean_description(remainder)

            # If we got a clean description, append matched words (if any)
            if cleaned:
                return f"{cleaned} {' '.join(matched_words)}".strip() if matched_words else cleaned

            # If description line is unusable but matched words exist, return them
            if matched_words:
                return " ".join(matched_words)

            return None

    # No Description line found; optionally still return matched words if present
    return " ".join(matched_words) if matched_words else None


def extract_address_blocks(lines: List[Dict[str, Any]], image_width: int) -> List[str]:
    # Build state+ZIP regex from current US_STATE_ABBRS (keeps this modular)
    state_zip_re = re.compile(
        rf"\b(?:{'|'.join(map(re.escape, US_STATE_ABBRS))})\b\s*,?\s*\d{{5}}(?:-\d{{4}})?\b",
        re.IGNORECASE,
    )

    blocks: List[str] = []
    seen: set[str] = set()
    skip_re = re.compile(r"\b(fifa|county|commissioner|tax|court)\b", re.IGNORECASE)


    def _bbox_h(bbox):
        return max(1, int(bbox[3]) - int(bbox[1]))

    for i, ln in enumerate(lines):
        txt = (ln.get("text") or "").strip()
        if not txt:
            continue

        if not state_zip_re.search(txt):
            continue

        # Collect up to 3 preceding lines, but stop if the vertical gap is too large
        picked = [i]
        line_h = _bbox_h(ln["bbox"])
        max_gap = int(2.5 * line_h)

        j = i - 1
        while j >= 0 and len(picked) < 4:  # 3 lines above + current
            prev = lines[j]
            prev_txt = (prev.get("text") or "").strip()
            if not prev_txt:
                j -= 1
                continue

            prev_bbox = prev["bbox"]
            curr_bbox = lines[picked[-1]]["bbox"]
            gap = int(curr_bbox[1]) - int(prev_bbox[3])
            if gap > max_gap:
                break

            picked.append(j)
            j -= 1

        picked = sorted(picked)
        block_lines = [lines[k]["text"].strip() for k in picked if (lines[k].get("text") or "").strip()]
        block_text = "\n".join(block_lines).strip()
        if not block_text:
            continue

        # ---------------- CLEANUP ----------------
        cleaned = block_text
        if skip_re.search(cleaned):
            continue

        # 1) terminate anything after exact "GA 12345" / "GA, 12345" (only if exact match exists)
        m = state_zip_re.search(cleaned)
        if m:
            cleaned = cleaned[: m.end()]

        # 2) remove "location" occurrences (case-insensitive)
        cleaned = re.sub(r"\blocation\b\s*:?", " ", cleaned, flags=re.IGNORECASE)

        # 3) remove float-like numbers (e.g. 23.23, 58430.232)
        cleaned = re.sub(r"\b\d+\.\d+\b", " ", cleaned)

        # 4) keep only allowed special chars: comma and dot (remove everything else)
        cleaned = re.sub(r"[^A-Za-z0-9,\.\s\n]", " ", cleaned)

        # 5) convert newlines to commas (single line)
        cleaned = re.sub(r"\s*\n\s*", ", ", cleaned)

        # 6) collapse extra spaces + fix comma spacing
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
        cleaned = re.sub(r"\s*,\s*", ", ", cleaned)
        cleaned = re.sub(r"(, ){2,}", ", ", cleaned).strip(" ,")
        
        # 7) trim leading garbage by anchoring to an address-start before STATE+ZIP
        cleaned = _trim_to_address_span(cleaned, state_zip_re)
        if len(cleaned) < 10:
            continue

        # ---------------- DEDUPE ----------------
        norm = re.sub(r"\s+", " ", cleaned).strip().lower()
        if norm in seen:
            continue
        seen.add(norm)

        blocks.append(cleaned)

    return blocks



# ----------------- CORE PIPELINE ----------------- #
def process_cv2_image(img: np.ndarray) -> Dict[str, Any]:
    """
    takes an already-loaded OpenCV image.
    """
    print("Preprocessing image...")
    pre_text = preprocess_image(img, upscale=2.0)
    pre_data = preprocess_image(img, upscale=1.5)
    text, data = ensemble_ocr(img, preprocessed=pre_text, preprocessed_data=pre_data)
    
    print(f"********\n{text}\n********")
    
    ocr_lines = data_to_lines(data)
    amounts = extract_amounts(text)
    description = extract_description(text)
    addresses = extract_address_blocks(ocr_lines, image_width=int(pre_text.shape[1]))
    
    # If lower-res word boxes missed addresses, fallback once to hi-res boxes only
    if not addresses:
        data_hi = ocr_data(pre_text)
        ocr_lines_hi = data_to_lines(data_hi)
        addresses = extract_address_blocks(ocr_lines_hi, image_width=int(pre_text.shape[1]))

    return {
        "amounts": amounts,
        "addresses": addresses,
        "description": description,
    }


if __name__ == "__main__":
    folder = Path("ocr")
    png_paths = sorted(folder.glob("*.png"))

    for p in png_paths:
        print(p)
        img = cv2.imread(str(p))
        result = process_cv2_image(img)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print("-" * 40)


