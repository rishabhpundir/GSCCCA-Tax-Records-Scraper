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
from typing import Any, Dict, List

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

STATE_ZIP_RE = re.compile(
    rf"\b(?:{'|'.join(US_STATE_ABBRS)})\b\s*,?\s*\d{{5}}(?:-\d{{4}})?\b",
    re.IGNORECASE,
)
    

#----------------- OCR HELPERS ----------------- #
@lru_cache(maxsize=1)
def get_paddle_ocr():
    return PaddleOCR(
        lang="en",
        use_textline_orientation=True
    )

    
def _sim(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


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


def preprocess_image(img: np.ndarray, *, upscale: float = 2.5) -> np.ndarray:
    if img is None:
        raise ValueError("input image is None (cv2.imread failed)")

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Upscale helps a LOT for small fonts (your scan is ~1000px wide)
    if upscale and upscale != 1.0:
        gray = cv2.resize(gray, (0, 0), fx=upscale, fy=upscale, interpolation=cv2.INTER_CUBIC)

    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    gray = clahe.apply(gray)

    gray = cv2.fastNlMeansDenoising(gray, None, h=15, templateWindowSize=7, searchWindowSize=21)

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
    text = pytesseract.image_to_string(pil_img, config="--psm 6")
    return text


def ocr_data(img: np.ndarray) -> Dict[str, Any]:
    """Tesseract OCR that returns word-level data with bounding boxes."""
    pil_img = cv_to_pil(img)
    return pytesseract.image_to_data(
        pil_img,
        config="--oem 3 --psm 6",
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
        "TOTAL": 8,
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
    preprocessed = preprocess_image(img)
    data = ocr_data(preprocessed)
    ocr_lines = data_to_lines(data)

    text = ocr_image(preprocessed)
    amounts = extract_amounts(text)
    addresses = extract_address_blocks(ocr_lines, image_width=int(preprocessed.shape[1]))

    return {
        "amounts": amounts,
        "addresses": addresses,
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


