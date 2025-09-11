import os
import zipfile
import pytesseract
from pdf2image import convert_from_path
import cv2


pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

zip_path = "downloads.zip"         
extract_dir = "extracted_pdfs"    
output_dir = "output_texts"        
debug_img_dir = "debug_images"     

if not os.path.exists(extract_dir):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

os.makedirs(output_dir, exist_ok=True)
os.makedirs(debug_img_dir, exist_ok=True)

pdf_files = []
for root, dirs, files in os.walk(extract_dir):
    for f in files:
        if f.lower().endswith(".pdf"):
            pdf_files.append(os.path.join(root, f))

print(f"\n[INFO] Found {len(pdf_files)} PDFs")



def preprocess_image(img_path, save_name):
    """Light preprocessing for stable OCR"""
    img = cv2.imread(img_path, cv2.IMREAD_COLOR)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    debug_path = os.path.join(debug_img_dir, save_name)
    cv2.imwrite(debug_path, thresh)

    return thresh


def run_ocr(img):
    """Try limited OCR configs and return the best"""
    configs = [
        "--oem 3 --psm 6",  
        "--oem 3 --psm 4",
        "--oem 3 --psm 7",  # Single text line
        "--oem 3 --psm 8",  # Single word
        "--oem 3 --psm 13"  # Raw line (no segmentation)   
    ]
    results = []
    for cfg in configs:
        text = pytesseract.image_to_string(img, lang="eng", config=cfg)
        results.append(text)


    best_text = max(results, key=lambda t: len(t.strip()))
    return best_text.strip()


for pdf_path in pdf_files:
    file_name = os.path.basename(pdf_path)
    base_name = os.path.splitext(file_name)[0]
    txt_output_path = os.path.join(output_dir, f"{base_name}.txt")

    print(f"\n[INFO] Processing {file_name}...")

    try:
        pages = convert_from_path(pdf_path, dpi=300)
        print(f"[DEBUG] {len(pages)} pages found")

        full_text = ""

        for i, page in enumerate(pages):
            img_path = f"temp_page_{i}.jpg"
            page.save(img_path, "JPEG")

            processed_img = preprocess_image(img_path, f"{base_name}_page_{i}.png")
            text = run_ocr(processed_img)

            print(f"[DEBUG] Page {i+1}: {len(text)} characters extracted")

            full_text += f"\n\n--- Page {i+1} ---\n\n{text}"

            os.remove(img_path)

        with open(txt_output_path, "w", encoding="utf-8") as f:
            f.write(full_text if full_text.strip() else "[EMPTY OCR RESULT]")

        print(f"[SUCCESS] Saved → {txt_output_path}")

    except Exception as e:
        print(f"[ERROR] Failed to process {file_name}: {e}")

print("\n All PDFs processed. Check 'output_texts' (plain text results).")
