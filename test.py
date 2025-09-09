import cv2
import numpy as np
import pytesseract
import re

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

IMAGE_PATH = r"C:\Users\panka\Desktop\GSCCCA-TAX\GSCCCA-Tax-Records-Scraper\temp_downloads\temp2.png"


def preprocess_methods(img):
    methods = {}
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # 1. Otsu Threshold
    _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    methods["otsu"] = otsu

    # # 2. Adaptive Threshold
    # adap = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
    #                              cv2.THRESH_BINARY, 31, 15)
    # methods["adaptive"] = adap

    # 3. Morphology Clean
    kernel = np.ones((1,1), np.uint8)
    morph = cv2.morphologyEx(otsu, cv2.MORPH_OPEN, kernel)
    methods["morph"] = morph

    # 4. Inverted
    inverted = cv2.bitwise_not(otsu)
    methods["inverted"] = inverted

    return methods


def ocr_image(img):
    config = r'--oem 3 --psm 6'
    text = pytesseract.image_to_string(img, config=config)
    return text


def score_text(text):
    return len(re.findall(r"[A-Za-z0-9]", text))


def extract_total_due(ocr_text: str) -> str:
    fixed_text = (
        ocr_text.replace("PTOTALDUE", "TOTAL DUE")
                .replace("TOTALDUE", "TOTAL DUE")
                .replace("T0TAL", "TOTAL")
                .replace("TOTAI", "TOTAL")
    )
    patterns = [
        r"TOTAL\s*DUE\s*[:\]\)]?\s*\$?\s*([\dOQSs]+[\.,]?\d{0,2})",   
        r"TOTAL\s*DUE[^\d]{0,5}\$?([\dOQSs]+)",                      
    ]

    for pat in patterns:
        match = re.search(pat, fixed_text, re.IGNORECASE)
        if match:
            amount_raw = match.group(1)

            amount_clean = (
                amount_raw.replace("O", "0")
                          .replace("Q", "0")
                          .replace("S", "5")
                          .replace("s", "5")
                          .replace("l", "1")
                          .replace("I", "1")
            )

            # Ensure decimal format
            if "." not in amount_clean and len(amount_clean) > 2:
                amount_clean = amount_clean[:-2] + "." + amount_clean[-2:]

            return "$" + amount_clean

    return "NOT FOUND"


if __name__ == "__main__":
    img = cv2.imread(IMAGE_PATH)

    preprocessed_versions = preprocess_methods(img)

    results = {}
    for name, proc_img in preprocessed_versions.items():
        resized = cv2.resize(proc_img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
        text = ocr_image(resized)
        score = score_text(text)
        results[name] = {"text": text, "score": score}
        cv2.imwrite(f"debug_{name}.png", resized)

    # Best result choose karo
    best_method = max(results, key=lambda k: results[k]["score"])
    best_text = results[best_method]["text"]

    print("========== OCR RESULTS ==========")
    for name, data in results.items():
        print(f"\n--- Method: {name} | Score={data['score']} ---")
        print(data["text"])

    print("\n✅ BEST METHOD:", best_method)
    print("✅ FINAL OCR TEXT:\n", best_text)

    # Extraction
    extracted = {"TOTAL DUE": extract_total_due(best_text)}
    print("\n📌 Extracted Fields:", extracted)
